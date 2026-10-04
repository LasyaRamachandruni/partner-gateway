"""Retries with capped exponential backoff, full jitter and an overall deadline.

    result = await retry(call, policy=RetryPolicy(attempts=4), retryable=is_transient)

- Only errors that `retryable` accepts are retried. Errors like "vehicle not found"
  or "permission denied" fail immediately.
- The delay before attempt n is uniform in [0, min(cap, base * 2^n)] ("full
  jitter"). Spreading retries out keeps many clients from hammering a recovering
  service in lockstep.
- A deadline bounds the total time, retries included. The caller's latency budget
  wins over the attempt count.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 4  # total tries, including the first
    base_s: float = 0.05
    cap_s: float = 1.0
    deadline_s: float | None = 3.0

    def delay(self, attempt: int, rng: random.Random) -> float:
        """Delay before retry number `attempt` (1 = first retry)."""
        return rng.uniform(0, min(self.cap_s, self.base_s * 2 ** (attempt - 1)))


class RetriesExhausted(Exception):
    def __init__(self, attempts: int, last: BaseException):
        super().__init__(f"gave up after {attempts} attempt(s): {last!r}")
        self.attempts = attempts
        self.last = last


async def retry(call: Callable[[], Awaitable[T]], policy: RetryPolicy = RetryPolicy(),
                retryable: Callable[[BaseException], bool] = lambda e: True,
                rng: random.Random | None = None,
                sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
                on_retry: Callable[[int, BaseException], None] | None = None) -> T:
    rng = rng or random.Random()
    start = time.monotonic()
    for attempt in range(1, policy.attempts + 1):
        try:
            return await call()
        except Exception as exc:  # noqa: BLE001 - decided by `retryable`
            if not retryable(exc) or attempt == policy.attempts:
                if attempt > 1:
                    raise RetriesExhausted(attempt, exc) from exc
                raise
            delay = policy.delay(attempt, rng)
            if policy.deadline_s is not None and time.monotonic() - start + delay > policy.deadline_s:
                raise RetriesExhausted(attempt, exc) from exc
            if on_retry:
                on_retry(attempt, exc)
            await sleep(delay)
    raise AssertionError("unreachable")
