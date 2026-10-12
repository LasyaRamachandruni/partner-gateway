"""Vehicles reached through the Smartcar API, including Smartcar's Vehicle Simulator.

Smartcar's Vehicle Simulator gives a developer account simulated cars that answer
the real API (https://smartcar.com/docs/help/vehicle-simulator). Connect one through
Smartcar Connect in simulated mode, and the resulting access and refresh tokens
let this backend read and command it like a real car:

    PGW_VEHICLE_BACKEND=smartcar
    PGW_SMARTCAR_CLIENT_ID=...            # from the Smartcar dashboard
    PGW_SMARTCAR_CLIENT_SECRET=...
    PGW_SMARTCAR_REFRESH_TOKEN=...        # from the Connect flow (or PGW_SMARTCAR_ACCESS_TOKEN for a quick test)

Endpoints used (Smartcar API v2.0, metric units via `sc-unit-system: metric`):

    GET  /vehicles                       vehicle ids this token can access
    GET  /vehicles/{id}/vin
    GET  /vehicles/{id}/battery          {"percentRemaining": 0.0-1.0, "range": km}
    GET  /vehicles/{id}/odometer         {"distance": km}
    GET  /vehicles/{id}/location         {"latitude", "longitude"}
    GET  /vehicles/{id}/security         {"isLocked": bool, ...}
    POST /vehicles/{id}/security         {"action": "LOCK" | "UNLOCK"}
    POST /vehicles/{id}/charge           {"action": "START" | "STOP"}

Partners address cars by VIN, Smartcar by vehicle id, so the backend maps one to
the other at start-up. Commands run in the background and their results are
published to the bus like the simulated fleet's. Smartcar has no horn command, so
HONK_AND_FLASH fails with a clear reason.

Limits: the command dedupe set and the VIN map live in this process (run one
vehicle-service instance), and a refreshed Smartcar refresh token is kept in
memory only; store it back (Secret Manager) if the service must survive restarts
for longer than the token's lifetime.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Any

import httpx

from ..bus.base import Bus
from .backend import Vehicle, now_iso, publish_result

log = logging.getLogger(__name__)

ACTIONS: dict[str, tuple[str, str]] = {
    "LOCK": ("security", "LOCK"),
    "UNLOCK": ("security", "UNLOCK"),
    "START_CHARGING": ("charge", "START"),
    "STOP_CHARGING": ("charge", "STOP"),
}


class SmartcarError(Exception):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(f"{status} {code}: {message}")
        self.status, self.code, self.message = status, code, message

    @property
    def retryable(self) -> bool:
        return self.status >= 500 or self.status == 429


@dataclass
class SmartcarTokens:
    access_token: str | None
    refresh_token: str | None
    client_id: str | None
    client_secret: str | None
    auth_url: str = "https://auth.smartcar.com/oauth/token"
    expires_at: float = 0.0

    async def bearer(self, client: httpx.AsyncClient) -> str:
        if self.access_token and (self.expires_at == 0.0 or time.time() < self.expires_at - 60):
            return self.access_token
        if not (self.refresh_token and self.client_id and self.client_secret):
            raise SmartcarError(401, "AUTHENTICATION", "access token expired and no refresh token configured")
        r = await client.post(
            self.auth_url,
            data={"grant_type": "refresh_token", "refresh_token": self.refresh_token},
            auth=(self.client_id, self.client_secret),
        )
        if r.status_code != 200:
            raise SmartcarError(r.status_code, "AUTHENTICATION", f"token refresh failed: {r.text[:200]}")
        body = r.json()
        self.access_token = body["access_token"]
        self.refresh_token = body.get("refresh_token", self.refresh_token)  # Smartcar rotates refresh tokens
        self.expires_at = time.time() + float(body.get("expires_in", 7200))
        log.info("smartcar access token refreshed")
        return self.access_token


class SmartcarBackend:
    name = "smartcar"

    def __init__(
        self,
        bus: Bus,
        tokens: SmartcarTokens,
        *,
        client: httpx.AsyncClient | None = None,
        api_url: str = "https://api.smartcar.com/v2.0",
        attempts: int = 3,
        backoff_s: float = 0.5,
    ) -> None:
        self.bus = bus
        self.tokens = tokens
        self.client = client or httpx.AsyncClient(timeout=15.0)
        self.api_url = api_url.rstrip("/")
        self.attempts = attempts
        self.backoff_s = backoff_s
        self.ids: dict[str, str] = {}  # VIN -> Smartcar vehicle id
        self.seen: set[str] = set()
        self._tasks: set[asyncio.Task[None]] = set()

    async def _request(self, method: str, path: str, json: dict[str, Any] | None = None) -> dict[str, Any]:
        last: SmartcarError | None = None
        for attempt in range(self.attempts):
            headers = {"Authorization": f"Bearer {await self.tokens.bearer(self.client)}", "sc-unit-system": "metric"}
            try:
                r = await self.client.request(method, f"{self.api_url}{path}", json=json, headers=headers)
            except httpx.HTTPError as e:
                last = SmartcarError(503, "NETWORK", str(e))
            else:
                if r.status_code < 300:
                    body: dict[str, Any] = r.json() if r.content else {}
                    return body
                try:
                    err = r.json()
                except ValueError:
                    err = {}
                last = SmartcarError(
                    r.status_code,
                    str(err.get("code") or err.get("type") or "ERROR"),
                    str(err.get("description") or err.get("message") or r.text[:200]),
                )
                if r.status_code == 401 and self.tokens.refresh_token:
                    self.tokens.expires_at = 1.0  # force a refresh on the next attempt
                elif not last.retryable:
                    raise last
            await asyncio.sleep(self.backoff_s * 2**attempt)
        assert last is not None
        raise last

    async def start(self) -> None:
        """Map VINs to Smartcar vehicle ids."""
        listed = await self._request("GET", "/vehicles")
        for vid in listed.get("vehicles", []):
            vin = (await self._request("GET", f"/vehicles/{vid}/vin"))["vin"]
            self.ids[vin] = vid
        log.info("smartcar vehicles found", extra={"count": len(self.ids)})

    async def state(self, vin: str) -> Vehicle | None:
        vid = self.ids.get(vin)
        if vid is None:
            return None
        # battery, odometer and location are required: if Smartcar can't answer, the caller gets an error
        # (UNAVAILABLE over gRPC), never placeholder numbers. Lock status isn't available on every make.
        battery, odometer, location = await asyncio.gather(
            self._request("GET", f"/vehicles/{vid}/battery"),
            self._request("GET", f"/vehicles/{vid}/odometer"),
            self._request("GET", f"/vehicles/{vid}/location"),
        )
        v = Vehicle(
            vin,
            online=True,
            plugged_in=False,
            charging=False,
            reported_at=now_iso(),
            battery_percent=round(float(battery.get("percentRemaining", 0.0)) * 100),
            odometer_km=float(odometer.get("distance", 0.0)),
            latitude=float(location["latitude"]),
            longitude=float(location["longitude"]),
        )
        try:
            v.locked = bool((await self._request("GET", f"/vehicles/{vid}/security")).get("isLocked", False))
        except SmartcarError as e:
            log.info("lock status unavailable", extra={"vin": vin, "error": str(e)})
        return v

    def accept(self, command_id: str, vin: str, type_: str, partner_id: str, trace: dict[str, str]) -> bool:
        if command_id in self.seen:
            return False
        self.seen.add(command_id)
        self._spawn(self._run(command_id, vin, type_, partner_id, trace))
        return True

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> None:
        t = asyncio.create_task(coro)
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    async def _run(self, command_id: str, vin: str, type_: str, partner_id: str, trace: dict[str, str]) -> None:
        status, reason = "SUCCEEDED", None
        action = ACTIONS.get(type_)
        if action is None:
            status, reason = "FAILED", f"{type_} is not supported by the Smartcar backend"
        else:
            endpoint, verb = action
            try:
                await self._request("POST", f"/vehicles/{self.ids[vin]}/{endpoint}", json={"action": verb})
            except SmartcarError as e:
                status, reason = "FAILED", f"smartcar {e.code}: {e.message}"
        await publish_result(
            self.bus,
            command_id=command_id,
            vin=vin,
            type_=type_,
            partner_id=partner_id,
            trace=trace,
            status=status,
            reason=reason,
        )

    async def publish_telemetry(self) -> int:
        return 0  # state is read on demand from Smartcar

    async def close(self) -> None:
        for t in list(self._tasks):
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.client.aclose()
