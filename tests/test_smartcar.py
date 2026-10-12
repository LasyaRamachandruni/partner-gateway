"""The Smartcar backend against a fake Smartcar API (httpx.MockTransport).

These tests pin the request shapes (paths, actions, units header, token refresh)
and the mapping back to the vehicle service's model. They don't reach Smartcar.
"""

from __future__ import annotations

import json

import httpx
import pytest

from pgw.bus.memory import InMemoryBus
from pgw.vehicles.backend import RESULTS_TOPIC
from pgw.vehicles.smartcar import SmartcarBackend, SmartcarError, SmartcarTokens


class FakeSmartcar:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict | None, str]] = []
        self.locked = True
        self.fail_next: list[int] = []
        self.valid_token = "access-1"

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.url.host == "auth.smartcar.com":
            form = dict(x.split("=") for x in request.content.decode().split("&"))
            assert form["grant_type"] == "refresh_token" and request.headers["authorization"].startswith("Basic ")
            self.valid_token = "access-2"
            return httpx.Response(
                200, json={"access_token": "access-2", "refresh_token": "refresh-2", "expires_in": 7200}
            )
        body = json.loads(request.content) if request.content else None
        auth = request.headers.get("authorization")
        self.calls.append((request.method, request.url.path, body, request.headers.get("sc-unit-system", "")))
        if auth != f"Bearer {self.valid_token}":
            return httpx.Response(401, json={"type": "AUTHENTICATION", "description": "expired"})
        if self.fail_next:
            return httpx.Response(self.fail_next.pop(0), json={"type": "SERVER", "description": "try again"})
        path = request.url.path.removeprefix("/v2.0")
        routes = {
            "/vehicles": {"vehicles": ["sc-1", "sc-2"]},
            "/vehicles/sc-1/vin": {"vin": "5YJ3E1EA1KF000001"},
            "/vehicles/sc-2/vin": {"vin": "5YJ3E1EA1KF000002"},
            "/vehicles/sc-1/battery": {"percentRemaining": 0.62, "range": 301.2},
            "/vehicles/sc-1/odometer": {"distance": 12345.6},
            "/vehicles/sc-1/location": {"latitude": 37.39, "longitude": -122.08},
        }
        if path == "/vehicles/sc-1/security" and request.method == "GET":
            return httpx.Response(200, json={"isLocked": self.locked})
        if path == "/vehicles/sc-1/security" and request.method == "POST":
            self.locked = body["action"] == "LOCK"
            return httpx.Response(200, json={"status": "success"})
        if path == "/vehicles/sc-1/charge":
            return httpx.Response(
                409,
                json={"type": "VEHICLE_STATE", "code": "CHARGE_FAULT", "description": "Charging plug is not connected"},
            )
        if path in routes and request.method == "GET":
            return httpx.Response(200, json=routes[path])
        return httpx.Response(404, json={"type": "RESOURCE_NOT_FOUND", "description": path})


@pytest.fixture()
async def setup():
    fake = FakeSmartcar()
    bus = InMemoryBus()
    results: list[dict] = []

    async def collect(msg):
        results.append(msg.data)

    await bus.subscribe(RESULTS_TOPIC, "test", collect)
    tokens = SmartcarTokens("access-1", "refresh-1", "client", "secret", expires_at=0.0)
    backend = SmartcarBackend(
        bus, tokens, client=httpx.AsyncClient(transport=httpx.MockTransport(fake.handler)), backoff_s=0.0
    )
    await backend.start()
    yield fake, backend, bus, results
    await backend.close()
    await bus.close()


async def test_maps_vins_and_reads_state_in_metric_units(setup):
    fake, backend, *_ = setup
    assert backend.ids == {"5YJ3E1EA1KF000001": "sc-1", "5YJ3E1EA1KF000002": "sc-2"}
    v = await backend.state("5YJ3E1EA1KF000001")
    assert (v.battery_percent, v.odometer_km, v.latitude, v.longitude, v.locked) == (62, 12345.6, 37.39, -122.08, True)
    assert v.online and all(unit == "metric" for *_, unit in fake.calls)
    assert await backend.state("NOT-A-SMARTCAR-VIN") is None


async def test_commands_publish_results_like_the_simulated_fleet(setup):
    fake, backend, bus, results = setup
    vin = "5YJ3E1EA1KF000001"
    assert backend.accept("cmd_1", vin, "UNLOCK", "partner_1", {}) is True
    assert backend.accept("cmd_1", vin, "UNLOCK", "partner_1", {}) is False  # a retry isn't sent twice
    backend.accept("cmd_2", vin, "START_CHARGING", "partner_1", {})
    backend.accept("cmd_3", vin, "HONK_AND_FLASH", "partner_1", {})
    await _settle(backend, bus)
    by_id = {r["command_id"]: r for r in results}
    assert by_id["cmd_1"]["status"] == "SUCCEEDED" and fake.locked is False
    assert ("POST", "/v2.0/vehicles/sc-1/security", {"action": "UNLOCK"}, "metric") in fake.calls
    assert by_id["cmd_2"]["status"] == "FAILED" and "Charging plug is not connected" in by_id["cmd_2"]["reason"]
    assert by_id["cmd_3"]["status"] == "FAILED" and "not supported" in by_id["cmd_3"]["reason"]


async def test_refreshes_an_expired_token_and_retries_server_errors(setup):
    fake, backend, *_ = setup
    fake.valid_token = "access-2"  # Smartcar now rejects access-1
    fake.fail_next = [503]
    v = await backend.state("5YJ3E1EA1KF000001")
    assert v.battery_percent == 62
    assert backend.tokens.access_token == "access-2" and backend.tokens.refresh_token == "refresh-2"


async def test_unreachable_vehicle_data_is_an_error_not_made_up_numbers(setup):
    fake, backend, *_ = setup
    fake.fail_next = [500] * 20
    with pytest.raises(SmartcarError):
        await backend.state("5YJ3E1EA1KF000001")


async def _settle(backend, bus):
    import asyncio

    for _ in range(200):
        if not backend._tasks:
            break
        await asyncio.sleep(0.01)
    await bus.drain()
