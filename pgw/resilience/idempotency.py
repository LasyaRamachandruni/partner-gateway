"""Idempotency keys for partner requests that change something.

A partner's network can drop the response to "unlock the car" after the gateway
already acted. The partner retries, and without protection the car gets two commands.
With an `Idempotency-Key` header:

- the first request with a key runs, and its response is stored under (partner, key);
- a repeat with the same key and the same body returns the stored response, without running again;
- a repeat with the same key but a *different* body is rejected (422), because the
  key was reused for something else;
- a repeat that arrives while the first is still running gets 409, so the partner retries shortly.

Keys expire after `ttl_s` (24 hours by default). A shared store (Redis, a database
table with a unique constraint) replaces this in-memory one when running several replicas.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class Stored:
    body_hash: str
    status: int | None  # None while the first request is still running
    response: dict | None
    expires_at: float


class KeyReused(Exception):
    pass


class InProgress(Exception):
    pass


def body_hash(body: dict) -> str:
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass
class IdempotencyStore:
    ttl_s: float = 24 * 3600
    clock: Callable[[], float] = time.time
    _items: dict[tuple[str, str], Stored] = field(default_factory=dict)

    def begin(self, partner_id: str, key: str, body: dict) -> Stored | None:
        """Returns a stored response to replay, or None if the caller should run the request."""
        now = self.clock()
        k = (partner_id, key)
        h = body_hash(body)
        s = self._items.get(k)
        if s is not None and s.expires_at <= now:
            s = None
        if s is None:
            self._items[k] = Stored(h, None, None, now + self.ttl_s)
            return None
        if s.body_hash != h:
            raise KeyReused(f"Idempotency-Key {key!r} was already used with a different request body")
        if s.status is None:
            raise InProgress(f"a request with Idempotency-Key {key!r} is still being processed")
        return s

    def finish(self, partner_id: str, key: str, status: int, response: dict) -> None:
        s = self._items[(partner_id, key)]
        s.status, s.response = status, response

    def abandon(self, partner_id: str, key: str) -> None:
        """The request failed before doing anything: let the partner retry with the same key."""
        self._items.pop((partner_id, key), None)
