"""The gateway's client for the vehicle service: mTLS, deadlines, retries, circuit breaker.

Every call runs in this order:
1. The circuit breaker decides whether to try at all. When open, it fails fast.
2. The call gets a per-attempt deadline (`timeout_s`).
3. Transient failures (UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED) are retried
   with jittered backoff, inside an overall deadline.
4. Errors that a retry can't fix (NOT_FOUND, INVALID_ARGUMENT, PERMISSION_DENIED)
   come back immediately and don't count against the breaker.

SendCommand is safe to retry because the vehicle service deduplicates command
ids: a retry after a lost response can't run the command twice.

Trace context goes along as gRPC metadata.
"""

from __future__ import annotations

import time

import grpc

from ..obs import tracing
from ..obs.metrics import Metrics
from ..proto import vehicle_pb2 as pb
from ..proto import vehicle_pb2_grpc as rpc
from ..resilience.breaker import CircuitBreaker
from ..resilience.retry import RetryPolicy, retry

TRANSIENT = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.RESOURCE_EXHAUSTED}


class NotFound(Exception):
    pass


class UpstreamUnavailable(Exception):
    def __init__(self, message: str, retry_after_s: float = 1.0):
        super().__init__(message)
        self.retry_after_s = retry_after_s


def _transient(exc: BaseException) -> bool:
    return isinstance(exc, grpc.aio.AioRpcError) and exc.code() in TRANSIENT


class VehicleClient:
    def __init__(self, target: str, *, tls: tuple[bytes, bytes, bytes] | None = None,
                 server_name: str | None = None, timeout_s: float = 1.0,
                 policy: RetryPolicy = RetryPolicy(), breaker: CircuitBreaker | None = None,
                 metrics: Metrics | None = None, resilient: bool = True):
        """`tls` = (ca_pem, client_cert_pem, client_key_pem). `resilient=False` disables retries and the
        breaker (used only by the chaos experiment as the baseline to compare against)."""
        if tls:
            ca, cert, key = tls
            creds = grpc.ssl_channel_credentials(root_certificates=ca, private_key=key, certificate_chain=cert)
            options = [("grpc.ssl_target_name_override", server_name)] if server_name else []
            self.channel = grpc.aio.secure_channel(target, creds, options=options)
        else:
            self.channel = grpc.aio.insecure_channel(target)
        self.stub = rpc.VehicleServiceStub(self.channel)
        self.timeout_s = timeout_s
        self.policy = policy if resilient else RetryPolicy(attempts=1, deadline_s=None)
        self.metrics = metrics or Metrics()
        self.breaker = breaker or CircuitBreaker(
            "vehicle-service", on_state_change=lambda n, s: self.metrics.set_breaker(n, s))
        self.resilient = resilient
        self.metrics.set_breaker(self.breaker.name, self.breaker.state)

    async def _call(self, method: str, request):
        fn = getattr(self.stub, method)

        async def attempt():
            t0 = time.perf_counter()
            try:
                result = await fn(request, timeout=self.timeout_s, metadata=tuple(tracing.inject().items()))
            except grpc.aio.AioRpcError as e:
                self.metrics.upstream.labels(method, e.code().name).inc()
                raise
            finally:
                self.metrics.upstream_latency.labels(method).observe(time.perf_counter() - t0)
            self.metrics.upstream.labels(method, "OK").inc()
            return result

        async def with_retries():
            return await retry(attempt, self.policy, retryable=_transient,
                               on_retry=lambda n, e: self.metrics.retries.labels(method).inc())

        try:
            if self.resilient:
                return await self.breaker.call(
                    with_retries, counts_as_failure=lambda e: _transient(getattr(e, "last", e)))
            return await with_retries()
        except grpc.aio.AioRpcError as e:
            raise self._translate(e) from e
        except Exception as e:  # RetriesExhausted / BreakerOpen
            last = getattr(e, "last", None)
            if isinstance(last, grpc.aio.AioRpcError) and not _transient(last):
                raise self._translate(last) from e
            raise UpstreamUnavailable(str(e), getattr(e, "retry_after_s", 1.0)) from e

    @staticmethod
    def _translate(e: grpc.aio.AioRpcError) -> Exception:
        if e.code() == grpc.StatusCode.NOT_FOUND:
            return NotFound(e.details())
        if e.code() in TRANSIENT:
            return UpstreamUnavailable(e.details())
        return RuntimeError(f"vehicle service error {e.code().name}: {e.details()}")

    async def get_state(self, vin: str) -> pb.VehicleState:
        return await self._call("GetVehicleState", pb.GetVehicleStateRequest(vin=vin))

    async def send_command(self, command_id: str, vin: str, type_: str, partner_id: str) -> pb.SendCommandResponse:
        return await self._call("SendCommand", pb.SendCommandRequest(
            command_id=command_id, vin=vin, type=pb.CommandType.Value(type_), partner_id=partner_id))

    async def close(self) -> None:
        await self.channel.close()
