"""Redis implementations of the rate limiter and the idempotency store.

Both are single Lua scripts, so each decision is atomic in Redis: two gateway
replicas that check the same bucket (or claim the same idempotency key) at the
same moment are serialized by Redis, never interleaved. Both scripts read the
time from Redis itself (`TIME`), so clock differences between replicas don't
change the result.

Key layout (all keys are prefixed, default `pgw:`):

    pgw:rl:<bucket>            hash {tokens, ts}           expires when the bucket would be full again
    pgw:idem:<partner>:<key>   string (JSON record)        expires after the lease (running) or the TTL (done)
"""

from __future__ import annotations

import json
import logging
from typing import Any

from redis.asyncio import Redis

from ..resilience.idempotency import InProgress, KeyReused, Stored, body_hash
from ..resilience.ratelimit import Decision

log = logging.getLogger(__name__)

# Token bucket. KEYS[1] = bucket; ARGV = rate (tokens/s), burst, cost.
# Returns {allowed (0/1), tokens left, seconds until `cost` tokens are available} as strings,
# because Lua numbers returned to Redis are truncated to integers.
TOKEN_BUCKET = """
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil then
  tokens = burst
  ts = now
end
tokens = math.min(burst, tokens + math.max(0, now - ts) * rate)
local allowed = 0
local wait = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  wait = (cost - tokens) / rate
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('PEXPIRE', KEYS[1], math.ceil((burst / rate) * 1000) + 1000)
return {tostring(allowed), tostring(tokens), tostring(wait)}
"""

# Idempotency, step 1. KEYS[1] = record; ARGV = body hash, lease ms.
# Returns {"new"} when the caller owns the key now, otherwise {"existing", record JSON}.
IDEM_BEGIN = """
local current = redis.call('GET', KEYS[1])
if current then
  return {'existing', current}
end
redis.call('SET', KEYS[1], cjson.encode({h = ARGV[1], s = false}), 'PX', ARGV[2])
return {'new'}
"""

# Idempotency, step 2. KEYS[1] = record; ARGV = body hash, status, response JSON, ttl ms.
# Only completes a record that is still running with the same body.
IDEM_FINISH = """
local current = redis.call('GET', KEYS[1])
if current then
  local rec = cjson.decode(current)
  if rec.h ~= ARGV[1] then
    return 0
  end
end
redis.call('SET', KEYS[1], cjson.encode({h = ARGV[1], s = tonumber(ARGV[2]), r = ARGV[3]}),
           'PX', ARGV[4])
return 1
"""


class RedisRateLimiter:
    def __init__(self, redis: Redis, prefix: str = "pgw:") -> None:
        self.redis = redis
        self.prefix = prefix
        self._script = redis.register_script(TOKEN_BUCKET)

    async def take(self, key: str, rate: float, burst: int, cost: float = 1.0) -> Decision:
        allowed, tokens, wait = await self._script(keys=[f"{self.prefix}rl:{key}"], args=[rate, burst, cost])
        left = float(tokens)
        return Decision(allowed == b"1" or allowed == "1", int(left), float(wait), burst)


class RedisIdempotencyStore:
    def __init__(self, redis: Redis, *, ttl_s: float = 24 * 3600, lease_s: float = 30.0, prefix: str = "pgw:") -> None:
        self.redis = redis
        self.ttl_ms = int(ttl_s * 1000)
        self.lease_ms = int(lease_s * 1000)
        self.prefix = prefix
        self._begin = redis.register_script(IDEM_BEGIN)
        self._finish = redis.register_script(IDEM_FINISH)

    def _key(self, partner_id: str, key: str) -> str:
        return f"{self.prefix}idem:{partner_id}:{key}"

    async def begin(self, partner_id: str, key: str, body: dict[str, Any]) -> Stored | None:
        h = body_hash(body)
        result = await self._begin(keys=[self._key(partner_id, key)], args=[h, self.lease_ms])
        if _text(result[0]) == "new":
            return None
        rec = json.loads(_text(result[1]))
        if rec["h"] != h:
            raise KeyReused(f"Idempotency-Key {key!r} was already used with a different request body")
        if rec["s"] is False:
            raise InProgress(f"a request with Idempotency-Key {key!r} is still being processed")
        return Stored(rec["h"], int(rec["s"]), json.loads(rec["r"]), 0.0)  # r is kept as text: no float rounding

    async def finish(
        self, partner_id: str, key: str, body: dict[str, Any], status: int, response: dict[str, Any]
    ) -> None:
        # The script checks the body hash again, so a request whose lease ran out can't overwrite
        # the record of a different request that took the key over in the meantime.
        done = await self._finish(
            keys=[self._key(partner_id, key)], args=[body_hash(body), status, json.dumps(response), self.ttl_ms]
        )
        if not int(done):
            log.warning("idempotency key was taken over before the request finished", extra={"partner_id": partner_id})

    async def abandon(self, partner_id: str, key: str) -> None:
        await self.redis.delete(self._key(partner_id, key))


def _text(v: bytes | str) -> str:
    return v.decode() if isinstance(v, bytes) else v
