"""Build the deployable services from `Settings`: what `pgw gateway` and `pgw vehicle-service` run.

Kept apart from the CLI so tests (and the multi-replica test in particular) can
start real gateway instances with exactly the production wiring.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import httpx

from . import certs
from .auth.keys import FileKeyProvider, KeyProvider, KeyRing, SecretManagerKeyProvider, StaticKeyProvider
from .auth.partners import PartnerRegistry
from .bus.base import Bus
from .gateway.app import Gateway
from .gateway.upstream import VehicleClient, id_token_credentials
from .gateway.webhooks import WebhookDispatcher
from .settings import Settings
from .store.base import Stores
from .store.memory import InMemoryStores
from .vehicles.backend import VehicleBackend

log = logging.getLogger(__name__)


def key_provider(s: Settings) -> KeyProvider | None:
    if s.signing_keys is not None:
        return StaticKeyProvider(s.signing_keys.get_secret_value())
    if s.signing_keys_file is not None:
        return FileKeyProvider(s.signing_keys_file)
    if s.signing_keys_secret is not None:
        return SecretManagerKeyProvider(s.signing_keys_secret)
    return None


def open_stores(s: Settings) -> Stores:
    if s.database_url is not None and s.redis_url is not None:
        from .store.shared import SharedStores

        return SharedStores.connect(
            s.database_url.get_secret_value(),
            s.redis_url,
            pool_size=s.db_pool_size,
            redis_max_connections=s.redis_max_connections,
        )
    log.warning("no PGW_DATABASE_URL/PGW_REDIS_URL: in-memory stores, a single replica only")
    return InMemoryStores()


def open_bus(s: Settings, stores: Stores | None = None) -> Bus:
    if s.bus == "pubsub":
        import os

        from .bus.gcp import GcpPubSubBus

        return GcpPubSubBus(
            s.pubsub_project, create=bool(os.environ.get("PUBSUB_EMULATOR_HOST")), suffix=s.pubsub_suffix
        )
    if s.bus == "redis":
        from redis.asyncio import Redis

        from .bus.redis import RedisStreamsBus

        redis = getattr(stores, "redis", None)
        if redis is not None:
            return RedisStreamsBus(redis)
        if not s.redis_url:
            raise ValueError("PGW_BUS=redis needs PGW_REDIS_URL")
        return RedisStreamsBus(Redis.from_url(s.redis_url), owns_redis=True)
    from .bus.memory import InMemoryBus

    return InMemoryBus()


def vehicle_client(s: Settings, metrics: Any = None) -> VehicleClient:
    if s.vehicle_auth == "google-id-token":
        audience = "https://" + s.vehicle_service.split(":")[0]
        return VehicleClient(
            s.vehicle_service,
            credentials=id_token_credentials(audience),
            timeout_s=s.vehicle_timeout_s,
            metrics=metrics,
        )
    if s.vehicle_auth == "insecure":
        return VehicleClient(s.vehicle_service, timeout_s=s.vehicle_timeout_s, metrics=metrics)
    bundle = certs.load(s.certs_dir)
    return VehicleClient(
        s.vehicle_service,
        server_name="vehicle-service",
        timeout_s=s.vehicle_timeout_s,
        tls=(bundle.ca.cert_pem, bundle.client.cert_pem, bundle.client.key_pem),
        metrics=metrics,
    )


@dataclass
class GatewayRuntime:
    gateway: Gateway
    stores: Stores
    bus: Bus
    webhook_client: httpx.AsyncClient

    async def close(self) -> None:
        await self.gateway.stop()
        await self.gateway.vehicles.close()
        await self.bus.close()
        await self.webhook_client.aclose()
        await self.stores.close()


async def build_gateway(
    s: Settings, *, stores: Stores | None = None, vehicles: VehicleClient | None = None, bus: Bus | None = None
) -> GatewayRuntime:
    stores = stores or open_stores(s)
    bus = bus or open_bus(s, stores)
    provider = key_provider(s)
    keys = (
        await KeyRing.from_provider(provider, token_ttl_s=s.token_ttl_s)
        if provider
        else KeyRing(token_ttl_s=s.token_ttl_s)
    )
    if provider is None:
        log.warning("no signing key source configured: generated a key in memory (local use only)")
    client = vehicles or vehicle_client(s)
    webhook_client = httpx.AsyncClient()
    dispatcher = WebhookDispatcher(webhook_client, repo=stores.deliveries, poll_s=s.webhook_poll_s)
    registry = PartnerRegistry(stores.partners, cache_ttl_s=s.partner_cache_ttl_s)
    gw = Gateway(
        registry,
        client,
        bus,
        dispatcher,
        stores=stores,
        keys=keys,
        command_ttl_s=s.command_ttl_s,
        keys_refresh_s=s.keys_refresh_s,
    )
    return GatewayRuntime(gw, stores, bus, webhook_client)


async def build_backend(s: Settings, bus: Bus) -> VehicleBackend:
    if s.vehicle_backend == "smartcar":
        from .vehicles.smartcar import SmartcarBackend, SmartcarTokens

        tokens = SmartcarTokens(
            s.smartcar_access_token.get_secret_value() if s.smartcar_access_token else None,
            s.smartcar_refresh_token.get_secret_value() if s.smartcar_refresh_token else None,
            s.smartcar_client_id,
            s.smartcar_client_secret.get_secret_value() if s.smartcar_client_secret else None,
            auth_url=s.smartcar_auth_url,
        )
        backend = SmartcarBackend(bus, tokens, api_url=s.smartcar_api_url)
        await backend.start()
        return backend
    from .vehicles.fleet import Fleet

    return Fleet(bus, demo_vins(s.vehicles), time_scale=s.time_scale)


def demo_vins(n: int) -> list[str]:
    return [f"1FTVW1EL0{i:08d}" for i in range(n)]
