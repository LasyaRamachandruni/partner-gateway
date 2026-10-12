"""Partner registry: credentials, scopes, rate plans, webhooks and vehicle grants.

A partner is an outside company (an insurer, a charging network, a fleet manager)
integrating with the platform. For each partner the gateway knows:

- client credentials: the secret is stored only as a salted scrypt hash and compared in constant time;
- scopes: what the partner may do, `vehicles:read` and/or `vehicles:command`;
- vehicle grants: which VINs the vehicles' owners have agreed to share with this partner;
- a rate plan: sustained requests per second, plus a burst;
- a webhook URL and signing secrets. A list of secrets allows rotation: during
  rotation each delivery is signed with every current secret, so the partner can
  switch keys without missing a delivery.

Partners are stored in a `PartnerRepository` (Postgres when deployed). Every
authenticated request needs the partner record, so lookups by id are cached for
`cache_ttl_s` (a few seconds): disabling a partner or changing its grants reaches
every replica within that time.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import os
import secrets
import time
from collections.abc import Callable

from ..store.base import PartnerRepository
from ..store.memory import InMemoryPartners
from ..store.records import SCOPES, Partner

__all__ = ["SCOPES", "Partner", "PartnerRegistry", "check_secret", "hash_secret"]

_DUMMY_HASH = "scrypt$00000000000000000000000000000000$" + "0" * 64


def hash_secret(secret: str, salt: bytes | None = None) -> str:
    salt = salt or os.urandom(16)
    digest = hashlib.scrypt(secret.encode(), salt=salt, n=2**14, r=8, p=1, dklen=32)
    return f"scrypt${salt.hex()}${digest.hex()}"


def check_secret(secret: str, stored: str) -> bool:
    _, salt_hex, digest_hex = stored.split("$")
    candidate = hash_secret(secret, bytes.fromhex(salt_hex)).split("$")[2]
    return hmac.compare_digest(candidate, digest_hex)


class PartnerRegistry:
    def __init__(self, repo: PartnerRepository | None = None, *, cache_ttl_s: float = 5.0,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.repo: PartnerRepository = repo or InMemoryPartners()
        self.cache_ttl_s = cache_ttl_s
        self.clock = clock
        self._cache: dict[str, tuple[float, Partner | None]] = {}

    async def register(self, name: str, scopes: set[str], vins: set[str], *, rate_per_s: float = 5.0, burst: int = 10,
                       webhook_url: str | None = None) -> tuple[Partner, str]:
        """Create a partner. Returns (partner, client_secret); the secret is shown once and never stored."""
        unknown = scopes - SCOPES
        if unknown:
            raise ValueError(f"unknown scopes: {sorted(unknown)}")
        if rate_per_s <= 0 or burst < 1:
            raise ValueError("rate_per_s must be positive and burst at least 1")
        client_id = f"pc_{secrets.token_hex(8)}"
        secret = secrets.token_urlsafe(32)
        p = Partner(f"partner_{secrets.token_hex(4)}", name, client_id, await asyncio.to_thread(hash_secret, secret),
                    set(scopes), set(vins), rate_per_s, burst, webhook_url,
                    [f"whsec_{secrets.token_urlsafe(24)}"] if webhook_url else [])
        await self.repo.add(p)
        return p, secret

    async def authenticate(self, client_id: str, secret: str) -> Partner | None:
        p = await self.repo.by_client_id(client_id)
        # hash even for unknown ids, so response time doesn't reveal which client ids exist.
        # scrypt takes tens of milliseconds of CPU: run it off the event loop.
        ok = await asyncio.to_thread(check_secret, secret, p.secret_hash if p else _DUMMY_HASH)
        return p if (p and ok and p.enabled) else None

    async def by_id(self, partner_id: str) -> Partner | None:
        now = self.clock()
        hit = self._cache.get(partner_id)
        if hit and hit[0] > now:
            return hit[1]
        p = await self.repo.by_id(partner_id)
        self._cache[partner_id] = (now + self.cache_ttl_s, p)
        return p

    def invalidate(self, partner_id: str) -> None:
        self._cache.pop(partner_id, None)

    async def _require(self, partner_id: str) -> Partner:
        p = await self.repo.by_id(partner_id)
        if p is None:
            raise KeyError(f"no partner {partner_id}")
        return p

    async def rotate_webhook_secret(self, partner_id: str) -> str:
        """Add a new signing secret. Deliveries are signed with both until `retire_webhook_secret`."""
        p = await self._require(partner_id)
        new = f"whsec_{secrets.token_urlsafe(24)}"
        p.webhook_secrets.append(new)
        await self.repo.save(p)
        self.invalidate(partner_id)
        return new

    async def retire_webhook_secret(self, partner_id: str) -> None:
        p = await self._require(partner_id)
        if len(p.webhook_secrets) > 1:
            p.webhook_secrets.pop(0)
            await self.repo.save(p)
        self.invalidate(partner_id)

    async def set_enabled(self, partner_id: str, enabled: bool) -> None:
        p = await self._require(partner_id)
        p.enabled = enabled
        await self.repo.save(p)
        self.invalidate(partner_id)
