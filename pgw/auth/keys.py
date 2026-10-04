"""Token signing keys with zero-downtime rotation.

Tokens are JWTs signed with ES256 (ECDSA P-256). Each key has a `kid`, and every
token says which key signed it. Verifiers (the gateway, or a partner who checks
tokens itself) fetch the public keys from `/.well-known/jwks.json` and cache them.

Rotation happens in two steps, so no valid token is ever rejected:

1. `stage()`: create the next key and publish it in the JWKS, but don't sign with it yet.
   Verifiers that refresh their cache now learn it before any token uses it.
2. `rotate()`: start signing with the staged key. The old key stays published,
   verify-only, until every token it signed has expired (token lifetime + leeway).
   Then `prune()` removes it.
"""

from __future__ import annotations

import base64
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec


def _b64(n: int) -> str:
    return base64.urlsafe_b64encode(n.to_bytes(32, "big")).rstrip(b"=").decode()


@dataclass
class SigningKey:
    kid: str
    private: ec.EllipticCurvePrivateKey
    created_at: float
    status: str  # staged | active | retiring
    retire_after: float | None = None

    def public_jwk(self) -> dict:
        nums = self.private.public_key().public_numbers()
        return {"kty": "EC", "crv": "P-256", "kid": self.kid, "use": "sig", "alg": "ES256",
                "x": _b64(nums.x), "y": _b64(nums.y)}

    def private_pem(self) -> bytes:
        return self.private.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                          serialization.NoEncryption())


@dataclass
class KeyRing:
    token_ttl_s: int = 900
    leeway_s: int = 60
    clock: Callable[[], float] = time.time
    keys: list[SigningKey] = field(default_factory=list)

    def __post_init__(self):
        if not self.keys:
            self.keys.append(self._new("active"))

    def _new(self, status: str) -> SigningKey:
        return SigningKey(f"k-{uuid.uuid4().hex[:10]}", ec.generate_private_key(ec.SECP256R1()), self.clock(), status)

    @property
    def active(self) -> SigningKey:
        return next(k for k in self.keys if k.status == "active")

    def stage(self) -> SigningKey:
        staged = next((k for k in self.keys if k.status == "staged"), None)
        if staged is None:
            staged = self._new("staged")
            self.keys.append(staged)
        return staged

    def rotate(self) -> SigningKey:
        staged = self.stage()
        old = self.active
        old.status, old.retire_after = "retiring", self.clock() + self.token_ttl_s + self.leeway_s
        staged.status = "active"
        return staged

    def prune(self) -> list[str]:
        now = self.clock()
        gone = [k.kid for k in self.keys if k.status == "retiring" and k.retire_after <= now]
        self.keys = [k for k in self.keys if k.kid not in gone]
        return gone

    def verification_key(self, kid: str):
        self.prune()
        k = next((k for k in self.keys if k.kid == kid), None)
        return k.private.public_key() if k else None

    def jwks(self) -> dict:
        self.prune()
        return {"keys": [k.public_jwk() for k in self.keys]}
