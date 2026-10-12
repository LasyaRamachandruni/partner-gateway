"""The partner-facing REST API.

    POST /oauth/token                          client credentials -> access token (JWT)
    GET  /.well-known/jwks.json                public signing keys
    GET  /v1/vehicles                          VINs this partner may access
    GET  /v1/vehicles/{vin}                    current vehicle state           scope vehicles:read
    POST /v1/vehicles/{vin}/commands           send a command (202 + id)       scope vehicles:command
    GET  /v1/commands/{command_id}             command status
    POST /v1/webhooks/test                     send a test event to the partner's webhook
    GET  /v1/webhooks/deliveries               recent deliveries, including dead letters
    POST /v1/webhooks/deliveries/{id}/replay   redeliver a dead-lettered event
    GET  /healthz  /readyz  /metrics

Request flow: request id + trace span → authenticate (Bearer JWT) → rate limit
(per partner) → authorize (scope + the owner's grant for this VIN) → idempotency
(commands) → resilient call to the vehicle service → JSON. Errors share one shape:
{"error": {"code", "message", "request_id"}}.

A VIN the partner has no grant for returns 404, not 403. Telling a partner that
a vehicle exists, but isn't theirs, would leak information.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import logging
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from ..auth.keys import KeyRing
from ..auth.partners import Partner, PartnerRegistry
from ..auth.tokens import TokenError, TokenService
from ..bus.base import Bus
from ..obs import tracing
from ..obs.metrics import Metrics
from ..resilience.idempotency import InProgress, KeyReused
from ..store.base import Stores
from ..store.memory import InMemoryStores
from ..vehicles.fleet import COMMANDS, RESULTS_TOPIC
from .commands import CommandService
from .upstream import NotFound, UpstreamUnavailable, VehicleClient
from .webhooks import WebhookDispatcher

log = logging.getLogger(__name__)


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, headers: dict[str, str] | None = None):
        super().__init__(message)
        self.status, self.code, self.message, self.headers = status, code, message, headers or {}


class CommandRequest(BaseModel):
    type: str


class Gateway:
    """Everything the API needs, wired together. `start()`/`stop()` manage background work.

    A Gateway holds no state another replica needs: partners, commands, webhook
    deliveries, rate-limit buckets and idempotency keys are all in `stores`, and
    the signing keys come from the key ring's provider."""

    def __init__(
        self,
        partners: PartnerRegistry,
        vehicles: VehicleClient,
        bus: Bus,
        webhooks: WebhookDispatcher,
        *,
        stores: Stores | None = None,
        keys: KeyRing | None = None,
        metrics: Metrics | None = None,
        command_ttl_s: float = 120.0,
        token_rate: tuple[float, int] = (1.0, 10),
        keys_refresh_s: float = 60.0,
        sweep_every_s: float = 5.0,
    ):
        self.stores: Stores = stores or InMemoryStores()
        self.partners = partners
        self.vehicles = vehicles
        self.bus = bus
        self.metrics = metrics or vehicles.metrics
        self.keys = keys or KeyRing()
        self.tokens = TokenService(self.keys)
        self.webhooks = webhooks
        self.webhooks.metrics = self.metrics
        self.webhooks.partner_lookup = partners.by_id
        self.commands = CommandService(self.stores.commands, partners, webhooks, self.metrics, ttl_s=command_ttl_s)
        self.ratelimits = self.stores.ratelimits
        self.idempotency = self.stores.idempotency
        self.token_rate = token_rate  # slows down secret guessing per client id
        self.keys_refresh_s = keys_refresh_s
        self.sweep_every_s = sweep_every_s
        self.ready = False
        self._key_refresher: asyncio.Task[None] | None = None

    async def start(self) -> None:
        await self.bus.subscribe(RESULTS_TOPIC, "gateway-command-results", self.commands.on_result)
        self.webhooks.start()
        self.commands.start(self.sweep_every_s)
        if self.keys.provider is not None:
            self._key_refresher = asyncio.create_task(self._refresh_keys(), name="key-refresh")
        self.ready = True
        log.info("gateway started")

    async def _refresh_keys(self) -> None:
        while True:
            await asyncio.sleep(self.keys_refresh_s)
            await self.keys.refresh()

    async def stop(self) -> None:
        self.ready = False
        if self._key_refresher:
            self._key_refresher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._key_refresher
        await self.commands.stop()
        await self.webhooks.stop()


def _err(status: int, code: str, message: str, request: Request, headers: dict[str, str] | None = None) -> JSONResponse:
    return JSONResponse(
        {"error": {"code": code, "message": message, "request_id": getattr(request.state, "request_id", None)}},
        status_code=status,
        headers=headers,
    )


def create_app(gw: Gateway, manage_lifecycle: bool = True) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if manage_lifecycle:
            await gw.start()
        yield
        if manage_lifecycle:
            await gw.stop()

    app = FastAPI(title="Partner Gateway", version="1.0", lifespan=lifespan)
    app.state.gw = gw
    tracer = tracing.tracer("partner-gateway")

    @app.exception_handler(ApiError)
    async def _api_error(request: Request, exc: ApiError) -> JSONResponse:
        return _err(exc.status, exc.code, exc.message, request, exc.headers)

    @app.exception_handler(Exception)
    async def _unexpected(request: Request, exc: Exception) -> JSONResponse:
        # A store or bug failure: log it with the request id, answer in the common error shape.
        log.error(
            "unhandled error",
            exc_info=exc,
            extra={"request_id": getattr(request.state, "request_id", None), "path": request.url.path},
        )
        return _err(500, "internal_error", "internal error", request)

    @app.middleware("http")
    async def observe(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        request.state.request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        t0 = time.perf_counter()
        with tracer.start_as_current_span(request.method, context=tracing.extract(dict(request.headers))) as span:
            try:
                response = await call_next(request)
            except Exception:
                # counted as a 5xx before the error handler answers, so the availability SLI sees it
                path = getattr(request.scope.get("route"), "path", "unmatched")
                gw.metrics.requests.labels(path, request.method, "500").inc()
                gw.metrics.latency.labels(path, request.method).observe(time.perf_counter() - t0)
                raise
            # name spans and label metrics by route template (/v1/vehicles/{vin}), never the raw path,
            # so VINs don't explode cardinality
            path = getattr(request.scope.get("route"), "path", "unmatched")
            span.update_name(f"{request.method} {path}")
            span.set_attribute("http.status_code", response.status_code)
        gw.metrics.requests.labels(path, request.method, str(response.status_code)).inc()
        gw.metrics.latency.labels(path, request.method).observe(time.perf_counter() - t0)
        response.headers["X-Request-Id"] = request.state.request_id
        return response

    # -- auth helpers ---------------------------------------------------------------
    async def authenticate(request: Request, scope: str | None = None) -> Partner:
        auth = request.headers.get("authorization", "")
        if not auth.lower().startswith("bearer "):
            raise ApiError(401, "unauthorized", "missing bearer token", {"WWW-Authenticate": "Bearer"})
        try:
            claims = gw.tokens.verify(auth[7:].strip())
        except TokenError as e:
            raise ApiError(401, "invalid_token", str(e), {"WWW-Authenticate": 'Bearer error="invalid_token"'}) from e
        partner = await gw.partners.by_id(claims.partner_id)
        if partner is None or not partner.enabled:
            raise ApiError(401, "invalid_token", "partner is disabled")
        decision = await gw.ratelimits.take(f"partner:{partner.partner_id}", partner.rate_per_s, partner.burst)
        request.state.rate_headers = {
            "X-RateLimit-Limit": str(decision.limit),
            "X-RateLimit-Remaining": str(decision.remaining),
        }
        if not decision.allowed:
            gw.metrics.rate_limited.labels(partner.partner_id).inc()
            raise ApiError(
                429,
                "rate_limited",
                "too many requests",
                {"Retry-After": str(max(1, round(decision.retry_after_s + 0.499))), **request.state.rate_headers},
            )
        if scope and scope not in claims.scopes:
            raise ApiError(403, "insufficient_scope", f"this token lacks the '{scope}' scope")
        return partner

    def granted(partner: Partner, vin: str) -> None:
        if vin not in partner.vins:
            raise ApiError(404, "not_found", f"no vehicle {vin} for this partner")

    def unavailable(e: UpstreamUnavailable) -> ApiError:
        return ApiError(
            503,
            "upstream_unavailable",
            "the vehicle service is temporarily unavailable",
            {"Retry-After": str(max(1, round(e.retry_after_s + 0.499)))},
        )

    # -- OAuth ---------------------------------------------------------------------
    @app.post("/oauth/token")
    async def token(request: Request) -> JSONResponse:
        form = await request.form()
        client_id, secret = form.get("client_id"), form.get("client_secret")
        auth = request.headers.get("authorization", "")
        if auth.lower().startswith("basic "):
            try:
                client_id, secret = base64.b64decode(auth[6:]).decode().split(":", 1)
            except (binascii.Error, UnicodeDecodeError, ValueError):
                return JSONResponse({"error": "invalid_client"}, 401)
        if form.get("grant_type") != "client_credentials":
            return JSONResponse({"error": "unsupported_grant_type"}, 400)
        if not client_id or not secret:
            return JSONResponse({"error": "invalid_client"}, 401)
        if not (await gw.ratelimits.take(f"token:{client_id}", *gw.token_rate)).allowed:
            return JSONResponse({"error": "slow_down"}, 429, headers={"Retry-After": "1"})
        partner = await gw.partners.authenticate(str(client_id), str(secret))
        if partner is None:
            return JSONResponse({"error": "invalid_client"}, 401)
        requested = set(str(form.get("scope", "")).split()) or None
        try:
            access, ttl, scopes = gw.tokens.issue(partner, requested)
        except TokenError:
            return JSONResponse({"error": "invalid_scope"}, 400)
        return JSONResponse(
            {"access_token": access, "token_type": "Bearer", "expires_in": ttl, "scope": " ".join(sorted(scopes))},
            headers={"Cache-Control": "no-store"},
        )

    @app.get("/.well-known/jwks.json")
    async def jwks() -> JSONResponse:
        return JSONResponse(gw.keys.jwks(), headers={"Cache-Control": "public, max-age=300"})

    # -- vehicles ------------------------------------------------------------------
    @app.get("/v1/vehicles")
    async def list_vehicles(request: Request) -> JSONResponse:
        p = await authenticate(request, "vehicles:read")
        return JSONResponse({"vehicles": sorted(p.vins)}, headers=request.state.rate_headers)

    @app.get("/v1/vehicles/{vin}")
    async def vehicle(vin: str, request: Request) -> JSONResponse:
        p = await authenticate(request, "vehicles:read")
        granted(p, vin)
        try:
            s = await gw.vehicles.get_state(vin)
        except NotFound as e:
            raise ApiError(404, "not_found", f"no vehicle {vin} for this partner") from e
        except UpstreamUnavailable as e:
            raise unavailable(e) from e
        return JSONResponse(
            {
                "vin": s.vin,
                "online": s.online,
                "locked": s.locked,
                "battery_percent": s.battery_percent,
                "odometer_km": s.odometer_km,
                "location": {"latitude": s.latitude, "longitude": s.longitude},
                "reported_at": s.reported_at,
            },
            headers=request.state.rate_headers,
        )

    @app.post("/v1/vehicles/{vin}/commands", status_code=202)
    async def send_command(
        vin: str, body: CommandRequest, request: Request, idempotency_key: str | None = Header(None)
    ) -> JSONResponse:
        p = await authenticate(request, "vehicles:command")
        granted(p, vin)
        if body.type not in COMMANDS:
            raise ApiError(422, "invalid_command", f"type must be one of {', '.join(COMMANDS)}")
        payload = {"vin": vin, "type": body.type}
        if idempotency_key:
            try:
                replay = await gw.idempotency.begin(p.partner_id, idempotency_key, payload)
            except KeyReused as e:
                raise ApiError(422, "idempotency_key_reused", str(e)) from e
            except InProgress as e:
                raise ApiError(409, "request_in_progress", str(e), {"Retry-After": "1"}) from e
            if replay is not None and replay.status is not None:
                return JSONResponse(replay.response, replay.status, headers={"Idempotent-Replayed": "true"})
        try:
            cmd = await gw.commands.create(p.partner_id, vin, body.type)
        except Exception:
            if idempotency_key:
                await gw.idempotency.abandon(p.partner_id, idempotency_key)
            raise
        try:
            await gw.vehicles.send_command(cmd.command_id, vin, body.type, p.partner_id)
        except (UpstreamUnavailable, NotFound, RuntimeError) as e:
            await gw.commands.discard(cmd.command_id)
            if idempotency_key:
                await gw.idempotency.abandon(p.partner_id, idempotency_key)  # nothing happened: safe to retry
            if isinstance(e, UpstreamUnavailable):
                raise unavailable(e) from e
            if isinstance(e, NotFound):
                raise ApiError(404, "not_found", f"no vehicle {vin} for this partner") from e
            raise
        result = {**cmd.public(), "links": {"self": f"/v1/commands/{cmd.command_id}"}}
        if idempotency_key:
            await gw.idempotency.finish(p.partner_id, idempotency_key, payload, 202, result)
        return JSONResponse(
            result, 202, headers={**request.state.rate_headers, "Location": f"/v1/commands/{cmd.command_id}"}
        )

    @app.get("/v1/commands/{command_id}")
    async def command_status(command_id: str, request: Request) -> JSONResponse:
        p = await authenticate(request)
        c = await gw.commands.get(command_id, p.partner_id)
        if c is None:
            raise ApiError(404, "not_found", f"no command {command_id}")
        return JSONResponse(c.public(), headers=request.state.rate_headers)

    # -- webhooks ----------------------------------------------------------------------
    @app.post("/v1/webhooks/test")
    async def webhook_test(request: Request) -> JSONResponse:
        p = await authenticate(request)
        d = await gw.webhooks.enqueue(p, "ping", {"message": "test event"})
        if d is None:
            raise ApiError(409, "no_webhook", "no webhook URL is configured for this partner")
        return JSONResponse({"event_id": d.event_id}, 202)

    @app.get("/v1/webhooks/deliveries")
    async def deliveries(request: Request, status: str | None = None) -> dict[str, Any]:
        p = await authenticate(request)
        items = await gw.webhooks.repo.list_for_partner(p.partner_id, status, limit=100)
        return {
            "deliveries": [
                {
                    "event_id": d.event_id,
                    "type": d.event["type"],
                    "status": d.status,
                    "attempts": d.attempts,
                    "last_error": d.last_error,
                }
                for d in items
            ]
        }

    @app.post("/v1/webhooks/deliveries/{event_id}/replay")
    async def replay(event_id: str, request: Request) -> JSONResponse:
        p = await authenticate(request)
        d = await gw.webhooks.repo.get(event_id)
        if d is None or d.partner_id != p.partner_id:
            raise ApiError(404, "not_found", f"no delivery {event_id}")
        if d.status != "dead":
            raise ApiError(409, "not_dead_lettered", "only dead-lettered deliveries can be replayed")
        await gw.webhooks.replay(event_id)
        return JSONResponse({"event_id": event_id, "status": "pending"}, 202)

    # -- operations ------------------------------------------------------------------
    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz() -> JSONResponse:
        # Readiness reflects this replica and the stores it can't work without. An open circuit to the vehicle
        # service is reported but doesn't make the replica unready: every replica would drop out at once, taking
        # token issuance and command status down with it.
        stores_ok = True
        try:
            await asyncio.wait_for(gw.stores.ping(), timeout=2.0)
        except Exception:
            log.warning("readiness: a store is unreachable", exc_info=True)
            stores_ok = False
        ready = gw.ready and stores_ok
        return JSONResponse(
            {
                "ready": ready,
                "stores": "ok" if stores_ok else "unreachable",
                "vehicle_service_circuit": gw.vehicles.breaker.state.value,
            },
            200 if ready else 503,
        )

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(gw.metrics.render(), media_type="text/plain; version=0.0.4")

    return app
