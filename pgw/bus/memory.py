"""In-memory bus with Pub/Sub semantics: fan-out to subscriptions, at-least-once, retries, dead letters.

`duplicate_rate` redelivers a share of successfully handled messages, which tests
use to prove consumers are idempotent.
"""

from __future__ import annotations

import asyncio
import json
import random
import uuid
from collections import defaultdict

from .base import Handler, Message

DEAD_LETTER_SUFFIX = ".dead-letter"


class InMemoryBus:
    def __init__(self, max_deliveries: int = 5, base_backoff_s: float = 0.01, duplicate_rate: float = 0.0,
                 seed: int | None = None):
        self.max_deliveries = max_deliveries
        self.base_backoff_s = base_backoff_s
        self.duplicate_rate = duplicate_rate
        self.rng = random.Random(seed)
        self._subs: dict[str, list[tuple[str, asyncio.Queue]]] = defaultdict(list)
        self._tasks: list[asyncio.Task] = []
        self.dead_letters: dict[str, list[Message]] = defaultdict(list)
        self.published: dict[str, int] = defaultdict(int)
        self._inflight = 0

    async def publish(self, topic: str, data: dict, attributes: dict[str, str] | None = None) -> str:
        json.dumps(data)  # same constraint as the real thing: payloads must serialize
        mid = uuid.uuid4().hex
        self.published[topic] += 1
        for _, q in self._subs.get(topic, []):
            q.put_nowait(Message(mid, topic, dict(data), dict(attributes or {})))
        return mid

    async def subscribe(self, topic: str, subscription: str, handler: Handler) -> None:
        q: asyncio.Queue = asyncio.Queue()
        self._subs[topic].append((subscription, q))
        self._tasks.append(asyncio.create_task(self._worker(topic, q, handler), name=f"sub:{subscription}"))

    async def _worker(self, topic: str, q: asyncio.Queue, handler: Handler) -> None:
        while True:
            msg: Message = await q.get()
            self._inflight += 1
            try:
                await handler(msg)
            except Exception:  # noqa: BLE001 - nack: redeliver later or dead-letter
                if msg.delivery_attempt >= self.max_deliveries:
                    self.dead_letters[topic + DEAD_LETTER_SUFFIX].append(msg)
                else:
                    delay = self.base_backoff_s * 2 ** (msg.delivery_attempt - 1)
                    retry = Message(msg.message_id, topic, msg.data, msg.attributes, msg.delivery_attempt + 1)
                    asyncio.get_running_loop().call_later(delay, q.put_nowait, retry)
            else:
                if self.duplicate_rate and self.rng.random() < self.duplicate_rate:
                    q.put_nowait(Message(msg.message_id, topic, msg.data, msg.attributes, msg.delivery_attempt + 1))
            finally:
                self._inflight -= 1
                q.task_done()

    async def drain(self, timeout: float = 5.0) -> None:
        """Wait until every queued message (including scheduled retries) has been handled. Test helper."""
        loop = asyncio.get_running_loop()
        end = loop.time() + timeout
        while loop.time() < end:
            if self._inflight == 0 and all(q.empty() for subs in self._subs.values() for _, q in subs):
                await asyncio.sleep(self.base_backoff_s * 2 ** self.max_deliveries)  # pending call_later retries
                if self._inflight == 0 and all(q.empty() for subs in self._subs.values() for _, q in subs):
                    return
            await asyncio.sleep(0.005)
        raise TimeoutError("bus did not drain")

    async def close(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
