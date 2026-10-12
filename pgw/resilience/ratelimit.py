"""Per-partner rate limiting with token buckets.

Each partner gets a bucket that holds up to `burst` tokens and refills at
`rate` tokens per second. A request takes one token. With no tokens left, the
gateway answers 429 with a Retry-After header that says exactly when the next token
will be available, so a well-behaved client never has to guess.

Two implementations share the `RateLimitStore` interface:

- `InMemoryRateLimiter`: one process. Used by unit tests and the single-process sandbox.
- `pgw.store.redis.RedisRateLimiter`: the same algorithm as an atomic Lua script in
  Redis, so every gateway replica draws from one bucket per partner.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol


@dataclass(frozen=True)
class Decision:
    allowed: bool
    remaining: int
    retry_after_s: float  # 0 when allowed
    limit: int


class RateLimitStore(Protocol):
    async def take(self, key: str, rate: float, burst: int, cost: float = 1.0) -> Decision:
        """Take `cost` tokens from the bucket `key` (created full on first use)."""
        ...


def refill(tokens: float, updated: float, now: float, rate: float, burst: int) -> float:
    """Tokens in a bucket at `now`, given its level at `updated`."""
    return min(float(burst), tokens + max(0.0, now - updated) * rate)


def decide(tokens: float, rate: float, burst: int, cost: float) -> tuple[Decision, float]:
    """Apply one request to a bucket holding `tokens`. Returns the decision and the new level."""
    if tokens >= cost:
        left = tokens - cost
        return Decision(True, math.floor(left), 0.0, burst), left
    return Decision(False, 0, (cost - tokens) / rate, burst), tokens


@dataclass
class _Bucket:
    tokens: float
    updated: float


@dataclass
class InMemoryRateLimiter:
    clock: Callable[[], float] = time.monotonic
    _buckets: dict[str, _Bucket] = field(default_factory=dict)

    def check(self, key: str, rate: float, burst: int, cost: float = 1.0) -> Decision:
        now = self.clock()
        b = self._buckets.get(key)
        if b is None:
            b = self._buckets[key] = _Bucket(float(burst), now)
        level = refill(b.tokens, b.updated, now, rate, burst)
        decision, b.tokens = decide(level, rate, burst, cost)
        b.updated = now
        return decision

    async def take(self, key: str, rate: float, burst: int, cost: float = 1.0) -> Decision:
        return self.check(key, rate, burst, cost)

    def reset(self) -> None:
        self._buckets.clear()
