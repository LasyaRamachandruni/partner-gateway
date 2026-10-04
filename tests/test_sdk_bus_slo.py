"""Partner SDK, the in-memory bus, SLO burn-rate math and fleet behavior."""

import asyncio
import time

import httpx
import pytest

from partner_sdk import ApiError, InvalidSignature, PartnerClient, verify_signature
from pgw.bus.memory import InMemoryBus
from pgw.gateway.webhooks import signature_header
from pgw.obs.slo import SLOS, budget_remaining, burn_rate, evaluate
from pgw.vehicles.fleet import Fleet

from .test_gateway import _wait


# SDK ---------------------------------------------------------------------------

async def test_sdk_end_to_end(sys_, receiver):
    async with PartnerClient("http://gw.test", sys_.partner.client_id, sys_.secret,
                             transport=httpx.ASGITransport(sys_.app)) as pc:
        vins = await pc.list_vehicles()
        state = await pc.get_vehicle(vins[0])
        assert state["vin"] == vins[0]
        cmd = await pc.send_command(vins[0], "LOCK")
        done = await pc.wait_for_command(cmd["command_id"], poll_s=0.02)
        assert done["status"] == "succeeded"
        assert pc.token_fetches == 1  # token cached across calls
        with pytest.raises(ApiError) as e:
            await pc.send_command(vins[0], "TELEPORT")
        assert e.value.status == 422 and e.value.request_id


async def test_sdk_honors_retry_after_and_keeps_the_idempotency_key(sys_):
    seen = []
    real = httpx.ASGITransport(sys_.app)

    class Flaky(httpx.AsyncBaseTransport):
        """Drops the first response to the command after the gateway has acted on it."""

        async def handle_async_request(self, request):
            resp = await real.handle_async_request(request)
            if request.url.path.endswith("/commands"):
                seen.append(request.headers["idempotency-key"])
                if len(seen) == 1:
                    raise httpx.ReadError("connection reset", request=request)
            return resp

    sleeps = []

    async def record(s):
        sleeps.append(s)

    async with PartnerClient("http://gw.test", sys_.partner.client_id, sys_.secret, transport=Flaky(),
                             sleep=record) as pc:
        vin = sorted(sys_.partner.vins)[0]
        cmd = await pc.send_command(vin, "UNLOCK")
    assert len(seen) == 2 and seen[0] == seen[1]  # retried with the same key
    assert len(sys_.fleet.seen) == 1  # ...so the car got one command
    assert sys_.gateway.commands.commands[cmd["command_id"]]


async def test_sdk_refreshes_token_on_401(sys_):
    async with PartnerClient("http://gw.test", sys_.partner.client_id, sys_.secret,
                             transport=httpx.ASGITransport(sys_.app)) as pc:
        await pc.list_vehicles()
        sys_.gateway.keys.rotate()
        sys_.gateway.keys.keys = [sys_.gateway.keys.active]  # old key revoked immediately
        assert await pc.list_vehicles()
        assert pc.token_fetches == 2


def test_signature_verification():
    body = b'{"id":"evt_1"}'
    now = int(time.time())
    header = signature_header(body, ["whsec_a"], now)
    assert verify_signature(body, header, "whsec_a") == now
    with pytest.raises(InvalidSignature):
        verify_signature(body + b" ", header, "whsec_a")  # body changed
    with pytest.raises(InvalidSignature):
        verify_signature(body, header, "whsec_b")  # wrong secret
    with pytest.raises(InvalidSignature, match="replay"):
        verify_signature(body, signature_header(body, ["whsec_a"], now - 3600), "whsec_a")
    with pytest.raises(InvalidSignature):
        verify_signature(body, "garbage", "whsec_a")


# bus -------------------------------------------------------------------------------

async def test_bus_fanout_retry_and_dead_letter():
    bus = InMemoryBus(max_deliveries=3, base_backoff_s=0.001)
    a, b, attempts = [], [], []

    async def ok(m):
        a.append(m.data["n"])

    async def flaky(m):
        attempts.append(m.delivery_attempt)
        if m.data["n"] == 2:
            raise RuntimeError("poison")
        b.append(m.data["n"])

    await bus.subscribe("t", "sub-a", ok)
    await bus.subscribe("t", "sub-b", flaky)
    for n in (1, 2, 3):
        await bus.publish("t", {"n": n})
    await bus.drain()
    assert a == [1, 2, 3] and sorted(b) == [1, 3]  # a poison message doesn't block others
    assert [m.data["n"] for m in bus.dead_letters["t.dead-letter"]] == [2]
    assert attempts.count(3) == 1  # dead-lettered after the 3rd delivery
    await bus.close()


async def test_consumers_survive_heavy_redelivery(receiver):
    """With 50% of events redelivered, every command still ends once and partners get one webhook each."""
    from pgw import system

    bus = InMemoryBus(duplicate_rate=0.5, seed=3)
    s = await system.build(bus=bus, webhook_transport=httpx.ASGITransport(receiver.app))
    try:
        async with PartnerClient("http://gw.test", s.partner.client_id, s.secret,
                                 transport=httpx.ASGITransport(s.app)) as pc:
            vins = sorted(s.partner.vins)
            for vin in vins:
                await pc.send_command(vin, "LOCK")
        await _wait(lambda: len(receiver.received) >= len(vins))
        await bus.drain()
        await s.gateway.webhooks.drain()
        assert len(receiver.received) == len(vins)
        assert s.gateway.commands.duplicates_ignored > 0
    finally:
        await s.close()


# SLOs --------------------------------------------------------------------------------

def test_burn_rate_and_alerts():
    slo = SLOS["availability"]  # 99.9%: budget 0.1%
    assert burn_rate(1, 1000, slo) == pytest.approx(1.0)
    assert burn_rate(0, 0, slo) == 0
    fast = {"1h": (150, 10_000), "5m": (20, 1_000), "6h": (200, 60_000), "30m": (30, 5_000)}
    assert [r.severity for r in evaluate(slo, fast)] == ["page"]  # 15x over the last hour and still happening
    recovered = {**fast, "5m": (0, 1_000)}
    assert evaluate(slo, recovered) == []  # short window resets the page once fixed
    assert budget_remaining(5, 10_000, slo) == pytest.approx(0.5)


# fleet ---------------------------------------------------------------------------

async def test_fleet_ignores_repeated_command_ids():
    bus = InMemoryBus()
    results = []

    async def collect(m):
        results.append(m.data)

    await bus.subscribe("vehicle-command-results", "t", collect)
    fleet = Fleet(bus, ["V1"], time_scale=0.001)
    assert fleet.accept("c1", "V1", "LOCK", "p", {}) is True
    assert fleet.accept("c1", "V1", "LOCK", "p", {}) is False
    await asyncio.sleep(0.05)
    await bus.drain()
    assert [r["command_id"] for r in results] == ["c1"]
    await fleet.close()
    await bus.close()


async def test_offline_command_expires():
    bus = InMemoryBus()
    results = []

    async def collect(m):
        results.append(m.data)

    await bus.subscribe("vehicle-command-results", "t", collect)
    fleet = Fleet(bus, ["V1"], time_scale=0.001, command_ttl_s=10)
    fleet.set_online("V1", False)
    fleet.accept("c1", "V1", "UNLOCK", "p", {})
    await asyncio.sleep(0.05)
    await bus.drain()
    assert results[0]["status"] == "EXPIRED"
    await fleet.close()
    await bus.close()


def test_committed_prometheus_rules_match_the_slo_definitions():
    from pathlib import Path

    import yaml

    from pgw.obs.slo import RULES, prometheus_rules, render_rules

    committed = Path(__file__).parent.parent / "deploy" / "prometheus" / "slo-rules.yml"
    assert committed.read_text() == render_rules(), "run `python -m pgw.obs.slo` to regenerate"
    alerts = {r["alert"]: r for r in yaml.safe_load(committed.read_text())["groups"][1]["rules"]}
    fast = alerts["PartnerGatewayAvailabilityBudgetBurn1h"]
    assert "> 0.0144 and" in fast["expr"] and fast["labels"]["severity"] == "page"  # 14.4 x 0.1% budget
    assert len(prometheus_rules()["groups"][1]["rules"]) == len(RULES) * 3 + 2
