"""Rate limiting, retries, circuit breaking and idempotency."""

import asyncio
import random

import pytest

from pgw.resilience.breaker import BreakerOpen, CircuitBreaker, State
from pgw.resilience.idempotency import IdempotencyStore, InProgress, KeyReused
from pgw.resilience.ratelimit import RateLimiter
from pgw.resilience.retry import RetriesExhausted, RetryPolicy, retry


class Clock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


# rate limiting ----------------------------------------------------------------

def test_bucket_allows_burst_then_refills_at_rate():
    clock = Clock()
    rl = RateLimiter(rate=2.0, burst=3, clock=clock)
    assert [rl.check("p").allowed for _ in range(4)] == [True, True, True, False]
    denied = rl.check("p")
    assert denied.retry_after_s == pytest.approx(0.5)  # one token at 2/s
    clock.t += 0.5
    assert rl.check("p").allowed and not rl.check("p").allowed
    clock.t += 100
    assert rl.check("p").remaining == 2  # capped at burst (3), minus this request


def test_partners_have_separate_buckets():
    rl = RateLimiter(rate=1.0, burst=1, clock=Clock())
    assert rl.check("a").allowed and rl.check("b").allowed
    assert not rl.check("a").allowed


# retry -----------------------------------------------------------------------

def _run(coro):
    return asyncio.run(coro)


async def _no_sleep(_):
    pass


def test_retry_recovers_from_transient_failures():
    calls = []

    async def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("blip")
        return "ok"

    assert _run(retry(flaky, RetryPolicy(attempts=4), sleep=_no_sleep)) == "ok"
    assert len(calls) == 3


def test_retry_does_not_retry_permanent_errors():
    calls = []

    async def bad():
        calls.append(1)
        raise ValueError("vehicle not found")

    with pytest.raises(ValueError):
        _run(retry(bad, retryable=lambda e: isinstance(e, ConnectionError), sleep=_no_sleep))
    assert len(calls) == 1


def test_retry_gives_up_after_attempts():
    async def down():
        raise ConnectionError("down")

    with pytest.raises(RetriesExhausted) as e:
        _run(retry(down, RetryPolicy(attempts=3, deadline_s=None), sleep=_no_sleep))
    assert e.value.attempts == 3


def test_backoff_is_capped_full_jitter():
    p = RetryPolicy(base_s=0.1, cap_s=0.5)
    rng = random.Random(0)
    for attempt, ceiling in [(1, 0.1), (2, 0.2), (3, 0.4), (4, 0.5), (8, 0.5)]:
        delays = [p.delay(attempt, rng) for _ in range(500)]
        assert 0 <= min(delays) and max(delays) <= ceiling
        assert max(delays) > 0.8 * ceiling  # jitter actually spreads across the range


def test_retry_respects_deadline():
    async def down():
        raise ConnectionError("down")

    async def slow_sleep(_):
        pass

    with pytest.raises(RetriesExhausted):
        _run(retry(down, RetryPolicy(attempts=50, base_s=10, cap_s=10, deadline_s=1.0),
                   rng=random.Random(1), sleep=slow_sleep))


# circuit breaker --------------------------------------------------------------

async def _ok():
    return "ok"


async def _fail():
    raise ConnectionError("upstream down")


def test_breaker_opens_on_failure_rate_and_fails_fast():
    clock = Clock()
    changes = []
    b = CircuitBreaker("vehicles", failure_threshold=0.5, window=10, min_calls=4, open_s=5, clock=clock,
                       on_state_change=lambda n, s: changes.append(s))
    for fn in (_ok, _fail, _fail):
        try:
            _run(b.call(fn))
        except ConnectionError:
            pass
    assert b.state == State.CLOSED  # under min_calls: not a pattern yet
    with pytest.raises(ConnectionError):
        _run(b.call(_fail))
    assert b.state == State.OPEN
    with pytest.raises(BreakerOpen) as e:
        _run(b.call(_ok))  # fails fast without calling upstream
    assert e.value.retry_after_s == pytest.approx(5)
    assert changes == [State.OPEN]


def test_breaker_half_open_recovers_or_reopens():
    clock = Clock()
    b = CircuitBreaker("v", min_calls=2, window=2, open_s=5, half_open_trials=2, clock=clock)
    for _ in range(2):
        with pytest.raises(ConnectionError):
            _run(b.call(_fail))
    assert b.state == State.OPEN
    clock.t += 5
    with pytest.raises(ConnectionError):
        _run(b.call(_fail))  # trial fails: straight back to open
    assert b.state == State.OPEN
    clock.t += 5
    assert _run(b.call(_ok)) == "ok" and b.state == State.HALF_OPEN
    assert _run(b.call(_ok)) == "ok" and b.state == State.CLOSED


def test_client_errors_do_not_trip_the_breaker():
    b = CircuitBreaker("v", min_calls=2, window=2)

    async def not_found():
        raise KeyError("no such vehicle")

    for _ in range(5):
        with pytest.raises(KeyError):
            _run(b.call(not_found, counts_as_failure=lambda e: not isinstance(e, KeyError)))
    assert b.state == State.CLOSED


# idempotency -------------------------------------------------------------------

def test_idempotency_replays_and_rejects_reuse():
    clock = Clock()
    s = IdempotencyStore(ttl_s=60, clock=clock)
    body = {"type": "unlock"}
    assert s.begin("p1", "k1", body) is None
    with pytest.raises(InProgress):
        s.begin("p1", "k1", body)
    s.finish("p1", "k1", 202, {"command_id": "c1"})
    replay = s.begin("p1", "k1", body)
    assert replay.status == 202 and replay.response == {"command_id": "c1"}
    with pytest.raises(KeyReused):
        s.begin("p1", "k1", {"type": "lock"})
    assert s.begin("p2", "k1", body) is None  # keys are per partner
    clock.t += 61
    assert s.begin("p1", "k1", {"type": "lock"}) is None  # expired: key free again


def test_abandoned_requests_can_be_retried():
    s = IdempotencyStore()
    s.begin("p", "k", {"a": 1})
    s.abandon("p", "k")
    assert s.begin("p", "k", {"a": 1}) is None
