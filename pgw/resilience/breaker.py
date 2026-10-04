"""Circuit breaker for calls to an upstream service.

    CLOSED ──(failure rate over threshold, enough calls)──► OPEN
      ▲                                                       │ after `open_s`
      └──(trial calls succeed)── HALF_OPEN ◄──────────────────┘
                                   │ a trial call fails
                                   └──────────────► OPEN

When the vehicle service is failing, retrying every request only adds load to
something that is already down, and makes partners wait for timeouts. An open
breaker fails fast (the gateway returns 503 with Retry-After), and lets a few trial
calls through once `open_s` has passed to see whether the service has recovered.

The failure rate is measured over the last `window` calls. Below `min_calls`, the breaker
doesn't trip: two failures out of two calls is not a pattern.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")


class State(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class BreakerOpen(Exception):
    def __init__(self, name: str, retry_after_s: float):
        super().__init__(f"circuit '{name}' is open; retry in {retry_after_s:.1f}s")
        self.retry_after_s = retry_after_s


@dataclass
class CircuitBreaker:
    name: str
    failure_threshold: float = 0.5
    window: int = 20
    min_calls: int = 10
    open_s: float = 5.0
    half_open_trials: int = 3
    clock: Callable[[], float] = time.monotonic
    on_state_change: Callable[[str, State], None] | None = None
    state: State = State.CLOSED
    _results: deque = field(default_factory=deque)
    _opened_at: float = 0.0
    _trials_in_flight: int = 0
    _trial_successes: int = 0

    def _set(self, s: State) -> None:
        if s != self.state:
            self.state = s
            if self.on_state_change:
                self.on_state_change(self.name, s)

    def _before(self) -> None:
        if self.state == State.OPEN:
            remaining = self.open_s - (self.clock() - self._opened_at)
            if remaining > 0:
                raise BreakerOpen(self.name, remaining)
            self._set(State.HALF_OPEN)
            self._trials_in_flight = self._trial_successes = 0
        if self.state == State.HALF_OPEN:
            if self._trials_in_flight >= self.half_open_trials:
                raise BreakerOpen(self.name, 0.5)
            self._trials_in_flight += 1

    def _record(self, ok: bool) -> None:
        if self.state == State.HALF_OPEN:
            self._trials_in_flight -= 1
            if not ok:
                self._trip()
                return
            self._trial_successes += 1
            if self._trial_successes >= self.half_open_trials:
                self._results.clear()
                self._set(State.CLOSED)
            return
        self._results.append(ok)
        while len(self._results) > self.window:
            self._results.popleft()
        if len(self._results) >= self.min_calls:
            failure_rate = self._results.count(False) / len(self._results)
            if failure_rate >= self.failure_threshold:
                self._trip()

    def _trip(self) -> None:
        self._opened_at = self.clock()
        self._results.clear()
        self._set(State.OPEN)

    async def call(self, fn: Callable[[], Awaitable[T]],
                   counts_as_failure: Callable[[BaseException], bool] = lambda e: True) -> T:
        self._before()
        try:
            result = await fn()
        except Exception as exc:
            # client errors (bad VIN, permission) say nothing about upstream health
            self._record(not counts_as_failure(exc))
            raise
        self._record(True)
        return result
