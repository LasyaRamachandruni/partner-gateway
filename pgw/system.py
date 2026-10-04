"""Wire the whole system together in one process: certificates, bus, fleet, the vehicle
service (gRPC over mTLS), the gateway and a demo partner.

Used by the tests, the sandbox (`pgw sandbox`) and the chaos experiment (`pgw chaos`).
Every piece is the real implementation. Only the cars and the message broker are
simulated (the bus is in memory unless a Pub/Sub one is passed in).
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx

from . import certs
from .auth.partners import Partner, PartnerRegistry
from .bus.base import Bus
from .bus.memory import InMemoryBus
from .gateway.app import Gateway, create_app
from .gateway.upstream import VehicleClient
from .gateway.webhooks import WebhookDispatcher
from .obs.metrics import Metrics
from .resilience.breaker import CircuitBreaker
from .resilience.retry import RetryPolicy
from .vehicles.fleet import Fleet
from .vehicles.service import Chaos, serve


@dataclass
class System:
    fleet: Fleet
    bus: Bus
    server: object
    servicer: object
    port: int
    vehicles: VehicleClient
    gateway: Gateway
    app: object
    partner: Partner
    secret: str
    webhook_client: httpx.AsyncClient
    bundle: certs.Bundle

    async def close(self) -> None:
        await self.gateway.stop()
        await self.vehicles.close()
        await self.server.stop(grace=None)
        await self.fleet.close()
        await self.bus.close()
        await self.webhook_client.aclose()


async def build(*, vehicles: int = 10, time_scale: float = 0.01, chaos: Chaos | None = None, resilient: bool = True,
                bus: Bus | None = None, webhook_url: str | None = "http://partner.test/hooks",
                webhook_transport: httpx.AsyncBaseTransport | None = None,
                webhook_backoff_s: tuple[float, ...] = (0.01, 0.02, 0.04), retry: RetryPolicy | None = None,
                breaker: CircuitBreaker | None = None, timeout_s: float = 1.0, rate: tuple[float, int] = (50.0, 100),
                command_ttl_s: float = 120.0, start: bool = True, seed: int = 0,
                host: str = "127.0.0.1", port: int = 0) -> System:
    bundle = certs.generate()
    bus = bus or InMemoryBus(seed=seed)
    fleet = Fleet(bus, [f"1FTVW1EL{seed:01d}{i:08d}" for i in range(vehicles)], seed=seed, time_scale=time_scale)
    server, bound, servicer = await serve(fleet, port, host=host, chaos=chaos, allowed_clients={"partner-gateway"},
                                          tls=(bundle.ca.cert_pem, bundle.server.cert_pem, bundle.server.key_pem))
    metrics = Metrics()
    client = VehicleClient(f"{host}:{bound}", tls=(bundle.ca.cert_pem, bundle.client.cert_pem, bundle.client.key_pem),
                           server_name="vehicle-service", timeout_s=timeout_s, metrics=metrics,
                           policy=retry or RetryPolicy(attempts=4, base_s=0.02, cap_s=0.2, deadline_s=2.0),
                           breaker=breaker, resilient=resilient)
    registry = PartnerRegistry()
    vins = list(fleet.vehicles)
    partner, secret = registry.register("Demo Insurance Co", {"vehicles:read", "vehicles:command"},
                                        set(vins[: max(1, len(vins) - 2)]),  # the last two cars aren't shared
                                        rate_per_s=rate[0], burst=rate[1], webhook_url=webhook_url)
    webhook_client = httpx.AsyncClient(transport=webhook_transport)
    dispatcher = WebhookDispatcher(webhook_client, backoff_s=webhook_backoff_s, seed=seed)
    gw = Gateway(registry, client, bus, dispatcher, metrics=metrics, command_ttl_s=command_ttl_s)
    if start:
        await gw.start()
    app = create_app(gw, manage_lifecycle=False)
    return System(fleet, bus, server, servicer, bound, client, gw, app, partner, secret, webhook_client, bundle)
