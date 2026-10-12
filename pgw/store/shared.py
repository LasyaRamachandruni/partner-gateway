"""The shared stores a multi-replica deployment uses: Postgres for records, Redis for counters and keys."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from redis.asyncio import BlockingConnectionPool, Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from .redis import RedisIdempotencyStore, RedisRateLimiter
from .sql import SqlCommands, SqlDeliveries, SqlPartners, make_engine


@dataclass
class SharedStores:
    engine: AsyncEngine
    redis: Redis
    partners: SqlPartners
    commands: SqlCommands
    deliveries: SqlDeliveries
    ratelimits: RedisRateLimiter
    idempotency: RedisIdempotencyStore

    @classmethod
    def connect(
        cls,
        database_url: str,
        redis_url: str,
        *,
        pool_size: int = 10,
        redis_max_connections: int = 50,
        prefix: str = "pgw:",
        idempotency_ttl_s: float = 24 * 3600,
    ) -> SharedStores:
        engine = make_engine(database_url, pool_size=pool_size)
        sessions: async_sessionmaker[Any] = async_sessionmaker(engine, expire_on_commit=False)
        # A blocking pool: under a burst, requests wait briefly for a free connection instead of failing
        # with "Too many connections" (found by the load test).
        pool = BlockingConnectionPool.from_url(
            redis_url,
            max_connections=redis_max_connections,
            timeout=2.0,
            health_check_interval=30,
            socket_timeout=2.0,
            socket_connect_timeout=2.0,
        )
        redis = Redis(connection_pool=pool)
        return cls(
            engine,
            redis,
            SqlPartners(sessions),
            SqlCommands(sessions),
            SqlDeliveries(sessions),
            RedisRateLimiter(redis, prefix),
            RedisIdempotencyStore(redis, ttl_s=idempotency_ttl_s, prefix=prefix),
        )

    async def ping(self) -> None:
        await self.redis.ping()
        async with self.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

    async def close(self) -> None:
        await self.redis.aclose()
        await self.engine.dispose()
