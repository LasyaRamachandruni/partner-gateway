"""The Redis Streams bus: fan-out, load sharing inside a subscription, redelivery and dead letters."""

from __future__ import annotations

import asyncio
import uuid
from collections import Counter

import pytest
from redis.asyncio import Redis

from pgw.bus.redis import RedisStreamsBus

from .integration import REDIS_URL, needs_infra

pytestmark = [needs_infra, pytest.mark.integration]


@pytest.fixture()
async def redis():
    r = Redis.from_url(REDIS_URL)
    yield r
    await r.aclose()


def _bus(redis: Redis, prefix: str) -> RedisStreamsBus:
    return RedisStreamsBus(redis, prefix=prefix, retry_after_s=0.1, block_ms=50, max_deliveries=3)


async def _until(predicate, timeout: float = 5.0) -> None:
    for _ in range(int(timeout / 0.01)):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise TimeoutError


async def test_every_subscription_gets_each_message_and_consumers_share_one(redis):
    prefix = f"bus{uuid.uuid4().hex[:6]}:"
    a1, a2, b = _bus(redis, prefix), _bus(redis, prefix), _bus(redis, prefix)
    seen_a: Counter[str] = Counter()
    seen_b: list[dict] = []

    async def on_a(msg):
        seen_a[msg.data["n"]] += 1

    async def on_b(msg):
        seen_b.append(msg.data)

    await a1.subscribe("t", "group-a", on_a)
    await a2.subscribe("t", "group-a", on_a)  # a second replica in the same subscription
    await b.subscribe("t", "group-b", on_b)
    for n in range(30):
        await a1.publish("t", {"n": str(n)}, {"traceparent": "00-x"})
    await _until(lambda: sum(seen_a.values()) == 30 and len(seen_b) == 30)
    await asyncio.sleep(0.2)
    assert set(seen_a.values()) == {1}  # each message handled once in group-a, by one of its consumers
    for bus in (a1, a2, b):
        await bus.close()


async def test_failures_are_redelivered_then_dead_lettered(redis):
    prefix = f"bus{uuid.uuid4().hex[:6]}:"
    bus = _bus(redis, prefix)
    attempts: Counter[str] = Counter()

    async def flaky(msg):
        attempts[msg.data["id"]] += 1
        if msg.data["id"] == "poison" or attempts[msg.data["id"]] < 2:
            raise RuntimeError("handler failed")

    await bus.subscribe("t", "g", flaky)
    await bus.publish("t", {"id": "ok-on-retry"})
    await bus.publish("t", {"id": "poison"})
    await _until(lambda: attempts["poison"] == 3 and attempts["ok-on-retry"] == 2)
    await asyncio.sleep(0.3)
    assert attempts["poison"] == 3  # max_deliveries, then no more
    dead = await redis.xrange(f"{prefix}stream:t.dead-letter")
    assert len(dead) == 1 and b"poison" in dead[0][1][b"data"]
    pending = await redis.xpending(f"{prefix}stream:t", "g")
    assert pending["pending"] == 0
    await bus.close()
