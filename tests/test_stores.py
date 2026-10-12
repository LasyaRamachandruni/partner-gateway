"""One contract for every store implementation: in-memory always, Redis + Postgres when available.

The concurrency tests matter most for the shared stores: they fire many operations at
once through separate connections and check that exactly the right number succeed.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from pgw.resilience.idempotency import InProgress, KeyReused
from pgw.store.memory import InMemoryStores
from pgw.store.records import Command, Delivery, Partner

from .integration import fresh_shared_stores, migrate, needs_infra


@pytest.fixture(scope="session")
def _schema():
    migrate()


@pytest.fixture(params=["memory", pytest.param("shared", marks=[needs_infra, pytest.mark.integration])])
async def stores(request):
    if request.param == "memory":
        yield InMemoryStores()
        return
    request.getfixturevalue("_schema")
    s = await fresh_shared_stores()
    yield s
    await s.close()


def _partner(pid: str = "partner_1", client_id: str = "pc_1") -> Partner:
    return Partner(
        pid,
        "Acme",
        client_id,
        "scrypt$00$00",
        {"vehicles:read"},
        {"VIN1", "VIN2"},
        2.0,
        3,
        "https://acme.test/hooks",
        ["whsec_1"],
    )


# rate limits ---------------------------------------------------------------------


async def test_bucket_allows_burst_then_denies_with_retry_after(stores):
    results = [await stores.ratelimits.take("p", rate=2.0, burst=3) for _ in range(4)]
    assert [r.allowed for r in results] == [True, True, True, False]
    assert results[0].remaining == 2 and results[0].limit == 3
    assert 0.3 < results[3].retry_after_s <= 0.5  # one token at 2/s, minus the time the calls took


async def test_buckets_are_separate_and_refill(stores):
    assert (await stores.ratelimits.take("a", rate=20.0, burst=1)).allowed
    assert (await stores.ratelimits.take("b", rate=20.0, burst=1)).allowed
    assert not (await stores.ratelimits.take("a", rate=20.0, burst=1)).allowed
    await asyncio.sleep(0.06)
    assert (await stores.ratelimits.take("a", rate=20.0, burst=1)).allowed


async def test_concurrent_takes_never_exceed_the_burst(stores):
    results = await asyncio.gather(*(stores.ratelimits.take("hot", rate=0.001, burst=10) for _ in range(60)))
    assert sum(r.allowed for r in results) == 10


# idempotency ---------------------------------------------------------------------


async def test_idempotency_lifecycle(stores):
    idem, body = stores.idempotency, {"vin": "V", "type": "LOCK"}
    assert await idem.begin("p1", "k1", body) is None
    with pytest.raises(InProgress):
        await idem.begin("p1", "k1", body)
    await idem.finish("p1", "k1", body, 202, {"command_id": "c1", "created_at": 1760290000.123456})
    replay = await idem.begin("p1", "k1", body)
    assert replay.status == 202 and replay.response == {"command_id": "c1", "created_at": 1760290000.123456}
    with pytest.raises(KeyReused):
        await idem.begin("p1", "k1", {"vin": "V", "type": "UNLOCK"})
    assert await idem.begin("p2", "k1", body) is None  # keys are per partner
    await idem.abandon("p2", "k1")
    assert await idem.begin("p2", "k1", body) is None


async def test_concurrent_begins_let_exactly_one_request_run(stores):
    async def attempt():
        try:
            return "run" if await stores.idempotency.begin("p", "same-key", {"x": 1}) is None else "replay"
        except InProgress:
            return "in_progress"

    outcomes = await asyncio.gather(*(attempt() for _ in range(40)))
    assert outcomes.count("run") == 1 and outcomes.count("in_progress") == 39


# partners ------------------------------------------------------------------------


async def test_partner_round_trip(stores):
    p = _partner()
    await stores.partners.add(p)
    assert await stores.partners.by_client_id("pc_1") == p
    assert await stores.partners.by_id("partner_1") == p
    assert await stores.partners.by_id("nope") is None
    p.webhook_secrets.append("whsec_2")
    p.enabled = False
    await stores.partners.save(p)
    got = await stores.partners.by_id("partner_1")
    assert got.webhook_secrets == ["whsec_1", "whsec_2"] and not got.enabled
    with pytest.raises(Exception):  # noqa: B017  (ValueError in memory, IntegrityError in Postgres)
        await stores.partners.add(_partner("partner_2", "pc_1"))
    with pytest.raises(KeyError):
        await stores.partners.save(_partner("partner_missing", "pc_9"))


# commands ------------------------------------------------------------------------


async def test_command_completes_once(stores):
    now = time.time()
    await stores.commands.add(Command("cmd_1", "partner_1", "VIN1", "LOCK", "pending", now))
    results = await asyncio.gather(
        *(stores.commands.complete("cmd_1", s, None, now + 1) for s in ["succeeded", "failed"] * 10)
    )
    applied = [r for r in results if r is not None]
    assert len(applied) == 1
    stored = await stores.commands.get("cmd_1")
    assert stored.status == applied[0].status and stored.completed_at == pytest.approx(now + 1, abs=1e-3)
    assert await stores.commands.complete("cmd_unknown", "succeeded", None, now) is None
    with pytest.raises(ValueError):
        await stores.commands.complete("cmd_1", "pending", None, now)


async def test_expire_pending_only_touches_overdue_pending_commands(stores):
    now = time.time()
    await stores.commands.add(Command("old", "p", "V", "LOCK", "pending", now - 300))
    await stores.commands.add(Command("new", "p", "V", "LOCK", "pending", now))
    await stores.commands.add(Command("done", "p", "V", "LOCK", "succeeded", now - 300, now - 299))
    expired = await stores.commands.expire_pending(now - 120, "too slow", now)
    assert [c.command_id for c in expired] == ["old"] and expired[0].reason == "too slow"
    assert await stores.commands.expire_pending(now - 120, "too slow", now) == []  # once
    assert (await stores.commands.get("done")).status == "succeeded"
    await stores.commands.delete("new")
    assert await stores.commands.get("new") is None


# webhook outbox ------------------------------------------------------------------


def _delivery(eid: str, at: float, partner: str = "partner_1") -> Delivery:
    return Delivery(
        eid, partner, "https://acme.test/hooks", {"id": eid, "type": "ping"}, {}, next_attempt_at=at, created_at=at
    )


async def test_claims_are_leased_to_one_caller(stores):
    now = time.time()
    for i in range(20):
        await stores.deliveries.add(_delivery(f"evt_{i:02d}", now - 1))
    await stores.deliveries.add(_delivery("evt_later", now + 60))
    a, b = await asyncio.gather(stores.deliveries.claim_due(now, 15, 30), stores.deliveries.claim_due(now, 15, 30))
    ids_a, ids_b = {d.event_id for d in a}, {d.event_id for d in b}
    assert not ids_a & ids_b and len(ids_a | ids_b) == 20  # disjoint, and the future one isn't due
    assert await stores.deliveries.claim_due(now, 50, 30) == []  # all leased
    assert len(await stores.deliveries.claim_due(now + 31, 50, 30)) == 20  # leases ran out: claimable again


async def test_saving_an_attempt_releases_the_lease(stores):
    now = time.time()
    await stores.deliveries.add(_delivery("evt_1", now - 1))
    (d,) = await stores.deliveries.claim_due(now, 10, 30)
    d.attempts, d.last_error, d.next_attempt_at = 1, "HTTP 503", now - 0.5
    d.history.append({"attempt": 1, "error": "HTTP 503"})
    await stores.deliveries.save(d)
    (again,) = await stores.deliveries.claim_due(now, 10, 30)
    assert again.attempts == 1 and again.history == [{"attempt": 1, "error": "HTTP 503"}]
    again.status = "delivered"
    await stores.deliveries.save(again)
    assert await stores.deliveries.count_pending() == 0
    assert (await stores.deliveries.get("evt_1")).status == "delivered"


async def test_list_for_partner(stores):
    now = time.time()
    for i in range(5):
        await stores.deliveries.add(_delivery(f"evt_{i}", now + i))
    await stores.deliveries.add(_delivery("evt_other", now, partner="partner_2"))
    listed = await stores.deliveries.list_for_partner("partner_1", limit=3)
    assert [d.event_id for d in listed] == ["evt_2", "evt_3", "evt_4"]  # most recent, oldest first
    assert await stores.deliveries.list_for_partner("partner_1", status="dead") == []


async def test_ping(stores):
    await stores.ping()


@needs_infra
@pytest.mark.integration
def test_admin_commands_migrate_and_register_a_partner(_schema, monkeypatch, capsys):
    import json

    from pgw.cli import main

    from .integration import DATABASE_URL

    monkeypatch.setenv("PGW_DATABASE_URL", DATABASE_URL)
    monkeypatch.setenv("PGW_LOG_FORMAT", "text")
    assert main(["db", "upgrade"]) == 0
    assert main(["partners", "create", "--name", "CLI Co", "--demo-vins", "3", "--rate", "50", "--burst", "100"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["client_id"].startswith("pc_") and out["vins"] == 3 and out["client_secret"]


def test_partners_create_needs_a_database(monkeypatch):
    from pgw.cli import main

    monkeypatch.delenv("PGW_DATABASE_URL", raising=False)
    assert main(["partners", "create", "--name", "x"]) == 2
