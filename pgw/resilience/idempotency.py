"""Idempotency keys for partner requests that change something.

A partner's network can drop the response to "unlock the car" after the gateway
already acted. The partner retries, and without protection the car gets two commands.
With an `Idempotency-Key` header:

- the first request with a key runs, and its response is stored under (partner, key);
- a repeat with the same key and the same body returns the stored response, without running again;
- a repeat with the same key but a *different* body is rejected (422), because the
  key was reused for something else;
- a repeat that arrives while the first is still running gets 409, so the partner retries shortly.

Keys expire after `ttl_s` (24 hours by default). A request that is still "in progress"
holds the key only for `lease_s`: if the replica handling it dies, the key is free
again after the lease instead of being stuck for a day.

`InMemoryIdempotencyStore` serves one process. `pgw.store.redis.RedisIdempotencyStore`
implements the same rules as one atomic Lua script, so two replicas that receive
the same key at the same moment can't both run the request.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class Stored:
    body_hash: str
    status: int | None  # None while the first request is still running
    response: dict[str, Any] | None
    expires_at: float


class KeyReused(Exception):
    pass


class InProgress(Exception):
    pass


def body_hash(body: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class IdempotencyStore(Protocol):
    async def begin(self, partner_id: str, key: str, body: dict[str, Any]) -> Stored | None:
        """A stored response to replay, or None if the caller should run the request.

        Raises KeyReused (same key, different body) or InProgress (first request still running)."""
        ...

    async def finish(
        self, partner_id: str, key: str, body: dict[str, Any], status: int, response: dict[str, Any]
    ) -> None: ...

    async def abandon(self, partner_id: str, key: str) -> None:
        """The request failed before doing anything: let the partner retry with the same key."""
        ...


@dataclass
class InMemoryIdempotencyStore:
    ttl_s: float = 24 * 3600
    lease_s: float = 30.0
    clock: Callable[[], float] = time.time
    _items: dict[tuple[str, str], Stored] = field(default_factory=dict)

    def begin_sync(self, partner_id: str, key: str, body: dict[str, Any]) -> Stored | None:
        now = self.clock()
        k = (partner_id, key)
        h = body_hash(body)
        s = self._items.get(k)
        if s is not None and s.expires_at <= now:
            s = None  # expired, or an in-progress lease that ran out
        if s is None:
            self._items[k] = Stored(h, None, None, now + self.lease_s)
            return None
        if s.body_hash != h:
            raise KeyReused(f"Idempotency-Key {key!r} was already used with a different request body")
        if s.status is None:
            raise InProgress(f"a request with Idempotency-Key {key!r} is still being processed")
        return s

    async def begin(self, partner_id: str, key: str, body: dict[str, Any]) -> Stored | None:
        return self.begin_sync(partner_id, key, body)

    async def finish(
        self, partner_id: str, key: str, body: dict[str, Any], status: int, response: dict[str, Any]
    ) -> None:
        s = self._items.get((partner_id, key))
        if s is not None and s.body_hash != body_hash(body):
            return  # the key was taken over by a different request after this one's lease ran out
        self._items[(partner_id, key)] = Stored(body_hash(body), status, response, self.clock() + self.ttl_s)

    async def abandon(self, partner_id: str, key: str) -> None:
        self._items.pop((partner_id, key), None)
