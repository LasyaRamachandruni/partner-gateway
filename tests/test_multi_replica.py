"""Two gateway replicas, one Redis, one Postgres: the guarantees must hold across replicas.

Each replica is a separate Gateway built by `pgw.runtime.build_gateway` (the production
wiring) with its own connection pools, served by its own uvicorn server on its own port.
Requests alternate between the replicas over real HTTP. The vehicle service is real too
(gRPC over mTLS), and command results travel over the Redis Streams bus.

The control test runs the same rate-limit burst against two replicas with in-memory
stores, to show the check would catch per-replica state.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import uuid
from collections import Counter
from collections.abc import AsyncIterator
from dataclasses import dataclass

import httpx
import pytest
import uvicorn
from fastapi import FastAPI, Request
from redis.asyncio import Redis
from sqlalchemy import func, select

from pgw import certs
from pgw.auth.keys import KeySet
from pgw.bus.redis import RedisStreamsBus
from pgw.gateway.app import create_app
from pgw.runtime import GatewayRuntime, build_gateway
from pgw.settings import Settings
from pgw.store.memory import InMemoryStores
from pgw.store.sql import CommandRow
from pgw.vehicles.fleet import Fleet
from pgw.vehicles.service import serve

from .integration import REDIS_URL, connect_shared, fresh_shared_stores, migrate, needs_infra

pytestmark = [needs_infra, pytest.mark.integration]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@contextlib.asynccontextmanager
async def http_server(app: FastAPI) -> AsyncIterator[str]:
    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="off"))
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task


class Receiver:
    def __init__(self) -> None:
        self.app = FastAPI()
        self.events: list[dict] = []

        @self.app.post("/hooks")
        async def hooks(request: Request) -> dict:
            self.events.append(await request.json())
            return {"ok": True}


@dataclass
class Cluster:
    urls: list[str]
    replicas: list[GatewayRuntime]
    fleet: Fleet
    receiver: Receiver
    client_id: str
    secret: str
    vins: list[str]


@pytest.fixture(scope="module")
def _schema():
    migrate()


@pytest.fixture()
async def cluster(_schema, tmp_path) -> AsyncIterator[Cluster]:
    certs.write(certs.generate(), tmp_path)
    bundle = certs.load(tmp_path)
    prefix = f"pgwtest:{uuid.uuid4().hex[:8]}:"
    seed_stores = await fresh_shared_stores()  # empties the tables; reused as replica A's stores

    vehicle_bus = RedisStreamsBus(Redis.from_url(REDIS_URL), prefix=prefix, block_ms=50, owns_redis=True)
    fleet = Fleet(vehicle_bus, [f"1FTVW1EL0{i:08d}" for i in range(5)], time_scale=0.01)
    server, port, _ = await serve(
        fleet,
        0,
        allowed_clients={"partner-gateway"},
        tls=(bundle.ca.cert_pem, bundle.server.cert_pem, bundle.server.key_pem),
    )

    settings = Settings(
        signing_keys=KeySet.new().dumps(),
        vehicle_service=f"127.0.0.1:{port}",
        certs_dir=tmp_path,
        webhook_poll_s=0.05,
        partner_cache_ttl_s=0.0,
    )
    replicas = []
    for i in range(2):
        stores = seed_stores if i == 0 else connect_shared(prefix)
        stores.ratelimits.prefix = stores.idempotency.prefix = prefix
        bus = RedisStreamsBus(Redis.from_url(REDIS_URL), prefix=prefix, block_ms=50, owns_redis=True)
        rt = await build_gateway(settings, stores=stores, bus=bus)
        await rt.gateway.start()
        replicas.append(rt)

    receiver = Receiver()
    async with contextlib.AsyncExitStack() as stack:
        hook_url = await stack.enter_async_context(http_server(receiver.app))
        partner, secret = await replicas[0].gateway.partners.register(
            "Two-replica Insurance",
            {"vehicles:read", "vehicles:command"},
            set(fleet.vehicles),
            rate_per_s=0.01,
            burst=20,
            webhook_url=f"{hook_url}/hooks",
        )
        urls = [
            await stack.enter_async_context(http_server(create_app(rt.gateway, manage_lifecycle=False)))
            for rt in replicas
        ]
        yield Cluster(urls, replicas, fleet, receiver, partner.client_id, secret, sorted(fleet.vehicles))
    for rt in replicas:
        await rt.close()
    await server.stop(grace=None)
    await fleet.close()
    await vehicle_bus.close()


async def _token(http: httpx.AsyncClient, url: str, c: Cluster) -> str:
    r = await http.post(f"{url}/oauth/token", data={"grant_type": "client_credentials"}, auth=(c.client_id, c.secret))
    assert r.status_code == 200, r.text
    return str(r.json()["access_token"])


async def test_a_token_from_one_replica_works_on_the_other(cluster):
    async with httpx.AsyncClient() as http:
        token = await _token(http, cluster.urls[0], cluster)
        r = await http.get(f"{cluster.urls[1]}/v1/vehicles", headers={"Authorization": f"Bearer {token}"})
        assert r.status_code == 200 and r.json()["vehicles"] == cluster.vins
        jwks = [(await http.get(f"{u}/.well-known/jwks.json")).json() for u in cluster.urls]
        assert jwks[0] == jwks[1]


async def test_rate_limit_is_shared_across_replicas(cluster):
    async with httpx.AsyncClient() as http:
        h = {"Authorization": f"Bearer {await _token(http, cluster.urls[0], cluster)}"}
        rs = await asyncio.gather(*(http.get(f"{cluster.urls[i % 2]}/v1/vehicles", headers=h) for i in range(60)))
    codes = Counter(r.status_code for r in rs)
    assert codes == {200: 20, 429: 40}  # burst 20 for the partner, not 20 per replica
    served_by = Counter(str(r.url).rsplit(":", 1)[1].split("/")[0] for r in rs if r.status_code == 200)
    assert len(served_by) == 2  # both replicas served some of the 20


async def test_control_in_memory_stores_would_double_the_limit():
    """The failure the shared store prevents: each replica with its own bucket lets 2x the burst through."""
    replicas = [InMemoryStores(), InMemoryStores()]
    results = await asyncio.gather(*(replicas[i % 2].ratelimits.take("partner:p", 0.01, 20) for i in range(60)))
    assert sum(r.allowed for r in results) == 40


async def test_idempotency_key_holds_across_replicas(cluster):
    vin = cluster.vins[0]
    async with httpx.AsyncClient() as http:
        h = {"Authorization": f"Bearer {await _token(http, cluster.urls[0], cluster)}", "Idempotency-Key": "same-key"}
        first = await asyncio.gather(
            *(
                http.post(f"{cluster.urls[i % 2]}/v1/vehicles/{vin}/commands", json={"type": "UNLOCK"}, headers=h)
                for i in range(10)
            )
        )
        later = await asyncio.gather(
            *(
                http.post(f"{cluster.urls[i % 2]}/v1/vehicles/{vin}/commands", json={"type": "UNLOCK"}, headers=h)
                for i in range(4)
            )
        )
        reused = await http.post(f"{cluster.urls[1]}/v1/vehicles/{vin}/commands", json={"type": "LOCK"}, headers=h)
    accepted = [r for r in first + later if r.status_code == 202]
    assert {r.status_code for r in first} <= {202, 409}
    assert len({r.json()["command_id"] for r in accepted}) == 1  # one command, whichever replica answered
    assert all(r.status_code == 202 and r.headers["idempotent-replayed"] == "true" for r in later)
    assert reused.status_code == 422
    assert len(cluster.fleet.seen) == 1  # the car got one command
    async with cluster.replicas[0].stores.engine.connect() as conn:
        assert await conn.scalar(select(func.count()).select_from(CommandRow)) == 1


async def test_each_result_is_applied_once_and_notified_once(cluster):
    async with httpx.AsyncClient() as http:
        h = {"Authorization": f"Bearer {await _token(http, cluster.urls[0], cluster)}"}
        ids = []
        for i, vin in enumerate(cluster.vins):
            r = await http.post(f"{cluster.urls[i % 2]}/v1/vehicles/{vin}/commands", json={"type": "LOCK"}, headers=h)
            assert r.status_code == 202, r.text
            ids.append(r.json()["command_id"])
        for _ in range(500):
            done = [e for e in cluster.receiver.events if e["type"] == "command.completed"]
            if len(done) >= len(ids):
                break
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.3)  # time for any duplicate to show up
        statuses = [(await http.get(f"{cluster.urls[1]}/v1/commands/{cid}", headers=h)).json()["status"] for cid in ids]
    completed = Counter(e["data"]["command_id"] for e in cluster.receiver.events if e["type"] == "command.completed")
    assert completed == Counter(ids)  # every command exactly once, though both replicas consume results
    assert statuses == ["succeeded"] * len(ids)
    applied = [
        rt.gateway.metrics.events.labels("vehicle-command-results", "applied")._value.get() for rt in cluster.replicas
    ]
    assert sum(applied) == len(ids)
