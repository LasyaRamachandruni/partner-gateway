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
"""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass, field

SCOPES = {"vehicles:read", "vehicles:command"}


def hash_secret(secret: str, salt: bytes | None = None) -> str:
    salt = salt or os.urandom(16)
    digest = hashlib.scrypt(secret.encode(), salt=salt, n=2**14, r=8, p=1, dklen=32)
    return f"scrypt${salt.hex()}${digest.hex()}"


def check_secret(secret: str, stored: str) -> bool:
    _, salt_hex, digest_hex = stored.split("$")
    candidate = hash_secret(secret, bytes.fromhex(salt_hex)).split("$")[2]
    return hmac.compare_digest(candidate, digest_hex)


@dataclass
class Partner:
    partner_id: str
    name: str
    client_id: str
    secret_hash: str
    scopes: set[str]
    vins: set[str]
    rate_per_s: float = 5.0
    burst: int = 10
    webhook_url: str | None = None
    webhook_secrets: list[str] = field(default_factory=list)
    enabled: bool = True


@dataclass
class PartnerRegistry:
    partners: dict[str, Partner] = field(default_factory=dict)  # by client_id

    def register(self, name: str, scopes: set[str], vins: set[str], *, rate_per_s: float = 5.0, burst: int = 10,
                 webhook_url: str | None = None) -> tuple[Partner, str]:
        """Create a partner. Returns (partner, client_secret); the secret is shown once and never stored."""
        unknown = scopes - SCOPES
        if unknown:
            raise ValueError(f"unknown scopes: {sorted(unknown)}")
        client_id = f"pc_{secrets.token_hex(8)}"
        secret = secrets.token_urlsafe(32)
        p = Partner(f"partner_{secrets.token_hex(4)}", name, client_id, hash_secret(secret), set(scopes), set(vins),
                    rate_per_s, burst, webhook_url, [f"whsec_{secrets.token_urlsafe(24)}"] if webhook_url else [])
        self.partners[client_id] = p
        return p, secret

    def authenticate(self, client_id: str, secret: str) -> Partner | None:
        p = self.partners.get(client_id)
        # hash even for unknown ids, so response time doesn't reveal which client ids exist
        ok = check_secret(secret, p.secret_hash if p else hash_secret("x"))
        return p if (p and ok and p.enabled) else None

    def by_id(self, partner_id: str) -> Partner | None:
        return next((p for p in self.partners.values() if p.partner_id == partner_id), None)

    def rotate_webhook_secret(self, partner_id: str) -> str:
        """Add a new signing secret. Deliveries are signed with both until `retire_webhook_secret`."""
        p = self.by_id(partner_id)
        new = f"whsec_{secrets.token_urlsafe(24)}"
        p.webhook_secrets.append(new)
        return new

    def retire_webhook_secret(self, partner_id: str) -> None:
        p = self.by_id(partner_id)
        if len(p.webhook_secrets) > 1:
            p.webhook_secrets.pop(0)
