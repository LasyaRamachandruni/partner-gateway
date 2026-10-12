"""Redis Streams implementation of the bus.

Same semantics as Pub/Sub: each subscription (a consumer group) gets every message,
delivery is at least once, a failed message is redelivered after `retry_after_s`,
and after `max_deliveries` it moves to `<topic>.dead-letter`.

It exists so the system runs as separate processes with nothing but Redis (local
docker compose, the multi-replica load test), and so a deployment that already
runs Redis can skip Pub/Sub. In GCP the default stays Pub/Sub.

    stream   pgw:stream:<topic>              XADD, trimmed to about `maxlen` entries
    group    <subscription>                  one per subscription; consumers are processes
    retries  pgw:attempts:<subscription>     hash: message id -> failed attempts

A consumer that dies holding messages doesn't lose them: other consumers in the
group take them over with XAUTOCLAIM once they have been idle for `retry_after_s`.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import socket
import uuid
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from .base import Handler, Message

log = logging.getLogger(__name__)

DEAD_LETTER_SUFFIX = ".dead-letter"


class RedisStreamsBus:
    def __init__(
        self,
        redis: Redis,
        *,
        prefix: str = "pgw:",
        max_deliveries: int = 5,
        retry_after_s: float = 5.0,
        block_ms: int = 1000,
        batch: int = 50,
        maxlen: int = 100_000,
        owns_redis: bool = False,
    ) -> None:
        self.redis = redis
        self.owns_redis = owns_redis  # close the connection pool on close()
        self.prefix = prefix
        self.max_deliveries = max_deliveries
        self.retry_after_ms = int(retry_after_s * 1000)
        self.block_ms = block_ms
        self.batch = batch
        self.maxlen = maxlen
        self.consumer = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self._tasks: list[asyncio.Task[None]] = []

    def _stream(self, topic: str) -> str:
        return f"{self.prefix}stream:{topic}"

    async def publish(self, topic: str, data: dict[str, Any], attributes: dict[str, str] | None = None) -> str:
        mid = await self.redis.xadd(
            self._stream(topic),
            {"data": json.dumps(data), "attrs": json.dumps(attributes or {})},
            maxlen=self.maxlen,
            approximate=True,
        )
        return mid.decode() if isinstance(mid, bytes) else str(mid)

    async def subscribe(self, topic: str, subscription: str, handler: Handler) -> None:
        stream = self._stream(topic)
        try:
            await self.redis.xgroup_create(stream, subscription, id="$", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
        self._tasks.append(
            asyncio.create_task(self._consume(topic, stream, subscription, handler), name=f"sub:{subscription}")
        )

    async def _consume(self, topic: str, stream: str, group: str, handler: Handler) -> None:
        cursor = "0-0"
        while True:
            try:
                # messages other consumers (or this one) failed or abandoned
                reclaimed: Any = await self.redis.xautoclaim(
                    stream, group, self.consumer, self.retry_after_ms, start_id=cursor, count=self.batch
                )
                cursor = _text(reclaimed[0])
                for mid, fields in reclaimed[1]:
                    if fields:
                        await self._handle(topic, stream, group, _text(mid), fields, handler, redelivered=True)
                fresh: Any = await self.redis.xreadgroup(
                    group, self.consumer, {stream: ">"}, count=self.batch, block=self.block_ms
                )
                for _, entries in fresh or []:
                    await asyncio.gather(
                        *(self._handle(topic, stream, group, _text(mid), fields, handler) for mid, fields in entries)
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("redis bus consumer error; retrying", extra={"subscription": group})
                await asyncio.sleep(1.0)

    async def _handle(
        self,
        topic: str,
        stream: str,
        group: str,
        mid: str,
        fields: dict[Any, Any],
        handler: Handler,
        redelivered: bool = False,
    ) -> None:
        attempts_key = f"{self.prefix}attempts:{group}"
        previous = int(await self.redis.hget(attempts_key, mid) or 0) if redelivered else 0
        data = json.loads(fields.get(b"data") or fields.get("data") or "{}")
        attrs = json.loads(fields.get(b"attrs") or fields.get("attrs") or "{}")
        msg = Message(mid, topic, data, attrs, previous + 1)
        try:
            await handler(msg)
        except Exception:
            failures = await self.redis.hincrby(attempts_key, mid, 1)
            if failures >= self.max_deliveries:
                await self.redis.xadd(
                    self._stream(topic + DEAD_LETTER_SUFFIX),
                    {"data": json.dumps(data), "attrs": json.dumps(attrs), "source_id": mid},
                )
                await self._ack(stream, group, mid, attempts_key)
                log.warning("message dead-lettered", extra={"topic": topic, "message_id": mid, "attempts": failures})
            return  # left pending: XAUTOCLAIM redelivers it after retry_after
        await self._ack(stream, group, mid, attempts_key)

    async def _ack(self, stream: str, group: str, mid: str, attempts_key: str) -> None:
        async with self.redis.pipeline(transaction=False) as p:
            p.xack(stream, group, mid)
            p.hdel(attempts_key, mid)
            await p.execute()

    async def close(self) -> None:
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await t
        self._tasks.clear()
        if self.owns_redis:
            await self.redis.aclose()


def _text(v: bytes | str) -> str:
    return v.decode() if isinstance(v, bytes) else v
