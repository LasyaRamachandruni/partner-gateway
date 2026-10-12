"""The vehicle service: gRPC over mutual TLS.

Only clients with a certificate from the internal CA can connect, and among
those only the identities in `allowed_clients` (the gateway) may call it. Every
other service, even a trusted one, gets PERMISSION_DENIED.

On Cloud Run the platform terminates TLS and can't forward client certificates,
so there the service listens in plaintext (h2c) behind Cloud Run, which admits
only callers holding `roles/run.invoker` (the gateway's service account, with a
Google-signed ID token). `serve(tls=None)` is that mode.

The cars behind the service are a `VehicleBackend`: the simulated fleet, or Smartcar.

`Chaos` makes the service misbehave on purpose: a share of calls fail with
UNAVAILABLE, and extra latency can be added. It is used to show how the gateway's
retries and circuit breaker behave when the service is unhealthy (see `pgw chaos`).
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field

import grpc

from ..obs import tracing
from ..proto import vehicle_pb2 as pb
from ..proto import vehicle_pb2_grpc as rpc
from .backend import COMMANDS, VehicleBackend


@dataclass
class Chaos:
    failure_rate: float = 0.0  # share of calls answered with UNAVAILABLE
    extra_latency_s: float = 0.0
    down: bool = False  # every call fails
    seed: int = 0
    rng: random.Random = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.rng = random.Random(self.seed)


class VehicleServicer(rpc.VehicleServiceServicer):
    def __init__(self, backend: VehicleBackend, chaos: Chaos | None = None,
                 allowed_clients: set[str] | None = None) -> None:
        self.backend = backend
        self.chaos = chaos or Chaos()
        self.allowed = allowed_clients
        self.calls = 0

    async def _guard(self, context: grpc.aio.ServicerContext) -> None:
        self.calls += 1
        if self.allowed is not None:
            cn = [v.decode() for v in context.auth_context().get("x509_common_name", [])]
            if not set(cn) & self.allowed:
                await context.abort(grpc.StatusCode.PERMISSION_DENIED, f"client {cn or '?'} is not allowed")
        if self.chaos.extra_latency_s:
            await asyncio.sleep(self.chaos.extra_latency_s)
        if self.chaos.down or (self.chaos.failure_rate and self.chaos.rng.random() < self.chaos.failure_rate):
            await context.abort(grpc.StatusCode.UNAVAILABLE, "vehicle service unavailable (chaos)")

    @staticmethod
    def _trace(context: grpc.aio.ServicerContext) -> dict[str, str]:
        return {k: v for k, v in context.invocation_metadata() if k == "traceparent" or k == "tracestate"}

    async def GetVehicleState(self, request: pb.GetVehicleStateRequest,
                              context: grpc.aio.ServicerContext) -> pb.VehicleState:
        await self._guard(context)
        with tracing.tracer("vehicle-service").start_as_current_span(
                "VehicleService/GetVehicleState", context=tracing.extract(self._trace(context))):
            try:
                v = await self.backend.state(request.vin)
            except Exception as e:
                await context.abort(grpc.StatusCode.UNAVAILABLE, f"vehicle backend error: {e}")
                raise AssertionError("unreachable") from e  # abort raises
            if v is None:
                await context.abort(grpc.StatusCode.NOT_FOUND, f"unknown vehicle {request.vin}")
                raise AssertionError("unreachable")  # abort raises
            return pb.VehicleState(vin=v.vin, online=v.online, locked=v.locked, battery_percent=v.battery_percent,
                                   odometer_km=v.odometer_km, latitude=v.latitude, longitude=v.longitude,
                                   reported_at=v.reported_at)

    async def SendCommand(self, request: pb.SendCommandRequest,
                          context: grpc.aio.ServicerContext) -> pb.SendCommandResponse:
        await self._guard(context)
        with tracing.tracer("vehicle-service").start_as_current_span(
                "VehicleService/SendCommand", context=tracing.extract(self._trace(context))):
            if await self.backend.state(request.vin) is None:
                await context.abort(grpc.StatusCode.NOT_FOUND, f"unknown vehicle {request.vin}")
            type_ = pb.CommandType.Name(request.type)
            if type_ not in COMMANDS:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"unsupported command {type_}")
            if not request.command_id:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "command_id is required")
            new = self.backend.accept(request.command_id, request.vin, type_, request.partner_id, tracing.inject())
            return pb.SendCommandResponse(command_id=request.command_id, accepted=True, duplicate=not new)


async def serve(backend: VehicleBackend, port: int = 0, *, tls: tuple[bytes, bytes, bytes] | None = None,
                chaos: Chaos | None = None, allowed_clients: set[str] | None = None,
                host: str = "127.0.0.1") -> tuple[grpc.aio.Server, int, VehicleServicer]:
    """Start the server. `tls` = (ca_pem, server_cert_pem, server_key_pem) enables mTLS (client certs required)."""
    server = grpc.aio.server()
    servicer = VehicleServicer(backend, chaos, allowed_clients)
    rpc.add_VehicleServiceServicer_to_server(servicer, server)
    addr = f"{host}:{port}"
    if tls:
        ca, cert, key = tls
        creds = grpc.ssl_server_credentials([(key, cert)], root_certificates=ca, require_client_auth=True)
        bound = server.add_secure_port(addr, creds)
    else:
        bound = server.add_insecure_port(addr)
    await server.start()
    return server, bound, servicer
