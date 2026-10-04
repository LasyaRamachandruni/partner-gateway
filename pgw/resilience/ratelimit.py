"""Per-partner rate limiting with token buckets.

Each partner gets a bucket that holds up to `burst` tokens and refills at
`rate` tokens per second. A request takes one token. With no tokens left, the
gateway answers 429 with a Retry-After header that says exactly when the next token
will be available, so a well-behaved client never has to guess.

This bucket is in memory. That works for a single gateway replica. With several
replicas, the same algorithm runs atomically in a shared store (a Redis Lua
script, for example). The `RateLimiter` interface stays the same.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class Decision:
    allowed: bool
    remaining: int
    retry_after_s: float  # 0 when allowed
    limit: int


@dataclass
class _Bucket:
    tokens: float
    updated: float


@dataclass
class RateLimiter:
    rate: float  # tokens per second
    burst: int
    clock: Callable[[], float] = time.monotonic
    _buckets: dict[str, _Bucket] = field(default_factory=dict)

    def check(self, key: str, cost: float = 1.0) -> Decision:
        now = self.clock()
        b = self._buckets.get(key)
        if b is None:
            b = self._buckets[key] = _Bucket(float(self.burst), now)
        b.tokens = min(self.burst, b.tokens + (now - b.updated) * self.rate)
        b.updated = now
        if b.tokens >= cost:
            b.tokens -= cost
            return Decision(True, math.floor(b.tokens), 0.0, self.burst)
        wait = (cost - b.tokens) / self.rate
        return Decision(False, 0, wait, self.burst)
