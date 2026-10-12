"""Helpers for tests that need real Redis and Postgres.

They run when PGW_TEST_REDIS_URL and PGW_TEST_DATABASE_URL are set (CI starts both as
service containers) and are skipped otherwise. Each test gets its own Redis key prefix
and empty tables.
"""

from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import text

from pgw.store.shared import SharedStores

REDIS_URL = os.environ.get("PGW_TEST_REDIS_URL")
DATABASE_URL = os.environ.get("PGW_TEST_DATABASE_URL")

needs_infra = pytest.mark.skipif(
    not (REDIS_URL and DATABASE_URL), reason="set PGW_TEST_REDIS_URL and PGW_TEST_DATABASE_URL"
)


def migrate() -> None:
    """Bring the test database to the latest schema (Alembic runs its own event loop, so use a thread)."""
    from concurrent.futures import ThreadPoolExecutor

    from pgw.migrations_cli import upgrade

    assert DATABASE_URL
    with ThreadPoolExecutor(1) as pool:
        pool.submit(upgrade, DATABASE_URL).result()


async def fresh_shared_stores(pool_size: int = 5) -> SharedStores:
    assert REDIS_URL and DATABASE_URL
    s = SharedStores.connect(DATABASE_URL, REDIS_URL, pool_size=pool_size, prefix=f"pgwtest:{uuid.uuid4().hex[:8]}:")
    async with s.engine.begin() as conn:
        await conn.execute(text("TRUNCATE partners, commands, webhook_deliveries"))
    return s


def connect_shared(prefix: str, pool_size: int = 5) -> SharedStores:
    """Another connection set to the same database and Redis prefix: what a second replica would have."""
    assert REDIS_URL and DATABASE_URL
    return SharedStores.connect(DATABASE_URL, REDIS_URL, pool_size=pool_size, prefix=prefix)
