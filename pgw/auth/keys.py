"""Token signing keys: where they come from, and how they rotate without downtime.

Tokens are JWTs signed with ES256 (ECDSA P-256). Each key has a `kid`, and every
token says which key signed it. Verifiers (the gateway replicas, or a partner who
checks tokens itself) fetch the public keys from `/.well-known/jwks.json`.

Keys live in a **key set**, a JSON document that names the active key and lists
every key that may still verify:

    {"active": "k-2026-10", "keys": [{"kid": "k-2026-10", "private_pem": "...", "retire_after": null}, ...]}

A `KeyProvider` loads the key set: from an environment variable or a file locally,
from Google Secret Manager in the cloud. Every replica reloads it every
`refresh_s`, so all replicas sign with the same key and publish the same JWKS.

Rotation is three edits of the key set (`pgw keys stage | activate | prune`), each
published as a new secret version:

1. stage: add the next key. It is published in the JWKS but nothing signs with it yet,
   so verifiers learn it before any token uses it.
2. activate: sign with the staged key. The old key stays, verify-only, until every
   token it signed has expired (`retire_after` = now + token lifetime + leeway).
3. prune: drop keys whose `retire_after` has passed.

For tests and the one-process sandbox, `KeyRing()` without a provider generates a
key in memory and supports the same three steps directly.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

log = logging.getLogger(__name__)


class KeyConfigError(Exception):
    pass


def _b64(n: int) -> str:
    return base64.urlsafe_b64encode(n.to_bytes(32, "big")).rstrip(b"=").decode()


def new_kid() -> str:
    return f"k-{uuid.uuid4().hex[:10]}"


@dataclass
class SigningKey:
    kid: str
    private: ec.EllipticCurvePrivateKey
    created_at: float
    status: str  # staged | active | retiring
    retire_after: float | None = None

    def public_jwk(self) -> dict[str, str]:
        nums = self.private.public_key().public_numbers()
        return {"kty": "EC", "crv": "P-256", "kid": self.kid, "use": "sig", "alg": "ES256",
                "x": _b64(nums.x), "y": _b64(nums.y)}

    def private_pem(self) -> bytes:
        return self.private.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                          serialization.NoEncryption())


def generate_key(status: str = "active", kid: str | None = None, now: float | None = None) -> SigningKey:
    return SigningKey(kid or new_kid(), ec.generate_private_key(ec.SECP256R1()), now or time.time(), status)


def _load_private(pem: str, kid: str) -> ec.EllipticCurvePrivateKey:
    key = serialization.load_pem_private_key(pem.encode(), password=None)
    if not isinstance(key, ec.EllipticCurvePrivateKey) or key.curve.name != "secp256r1":
        raise KeyConfigError(f"key {kid} is not an EC P-256 private key")
    return key


@dataclass
class KeySet:
    """The serialized form of the signing keys. Pure data: editing it never touches a running gateway."""

    active: str
    keys: list[dict[str, Any]]

    @classmethod
    def new(cls) -> KeySet:
        k = generate_key()
        return cls(k.kid, [{"kid": k.kid, "private_pem": k.private_pem().decode(), "retire_after": None}])

    @classmethod
    def parse(cls, raw: str | bytes) -> KeySet:
        try:
            doc = json.loads(raw)
            ks = cls(str(doc["active"]), list(doc["keys"]))
        except (ValueError, KeyError, TypeError) as e:
            raise KeyConfigError(f"not a key set: {e}") from e
        kids = [k.get("kid") for k in ks.keys]
        if len(set(kids)) != len(kids) or ks.active not in kids:
            raise KeyConfigError("key set must have unique kids and name an active key it contains")
        return ks

    def dumps(self) -> str:
        return json.dumps({"active": self.active, "keys": self.keys}, indent=2)

    def stage(self) -> str:
        """Add a new key, published but not signing. Returns its kid (an existing staged key is reused)."""
        for k in self.keys:
            if k["kid"] != self.active and k.get("retire_after") is None:
                return str(k["kid"])
        key = generate_key("staged")
        self.keys.append({"kid": key.kid, "private_pem": key.private_pem().decode(), "retire_after": None})
        return key.kid

    def activate(self, kid: str, *, token_ttl_s: int, leeway_s: int, now: float | None = None) -> None:
        """Sign with `kid` from now on; the previous active key verifies until its tokens have expired."""
        if kid not in {k["kid"] for k in self.keys}:
            raise KeyConfigError(f"no key {kid} in the key set; stage it first")
        now = now or time.time()
        for k in self.keys:
            if k["kid"] == self.active and kid != self.active:
                k["retire_after"] = now + token_ttl_s + leeway_s
            if k["kid"] == kid:
                k["retire_after"] = None
        self.active = kid

    def prune(self, now: float | None = None) -> list[str]:
        now = now or time.time()
        gone = [k["kid"] for k in self.keys if k.get("retire_after") is not None and k["retire_after"] <= now]
        self.keys = [k for k in self.keys if k["kid"] not in gone]
        return gone

    def signing_keys(self) -> list[SigningKey]:
        out = []
        for k in self.keys:
            status = "active" if k["kid"] == self.active else ("retiring" if k.get("retire_after") else "staged")
            out.append(SigningKey(k["kid"], _load_private(k["private_pem"], k["kid"]), 0.0, status,
                                  k.get("retire_after")))
        return out


class KeyProvider(Protocol):
    name: str

    async def load(self) -> KeySet: ...


@dataclass
class StaticKeyProvider:
    """A key set given directly, e.g. from the PGW_SIGNING_KEYS environment variable."""

    raw: str
    name: str = "env"

    async def load(self) -> KeySet:
        return KeySet.parse(self.raw)


@dataclass
class FileKeyProvider:
    """A key set in a file (a mounted secret, or a local file made with `pgw keys init`)."""

    path: Path
    name: str = "file"

    async def load(self) -> KeySet:
        return KeySet.parse(await asyncio.to_thread(self.path.read_bytes))


@dataclass
class SecretManagerKeyProvider:
    """A key set stored in Google Secret Manager. `secret` is projects/<p>/secrets/<name>[/versions/<v>]."""

    secret: str
    name: str = "secret-manager"
    _client: Any = None

    async def load(self) -> KeySet:
        from google.cloud import secretmanager  # optional dependency: pip install .[gcp]

        if self._client is None:
            self._client = secretmanager.SecretManagerServiceAsyncClient()
        name = self.secret if "/versions/" in self.secret else f"{self.secret}/versions/latest"
        resp = await self._client.access_secret_version(name=name)
        return KeySet.parse(resp.payload.data)


def provider_from_env(env: dict[str, str] | None = None) -> KeyProvider | None:
    """PGW_SIGNING_KEYS (JSON), PGW_SIGNING_KEYS_FILE, or PGW_SIGNING_KEYS_SECRET; None if none is set."""
    env = dict(os.environ) if env is None else env
    if env.get("PGW_SIGNING_KEYS"):
        return StaticKeyProvider(env["PGW_SIGNING_KEYS"])
    if env.get("PGW_SIGNING_KEYS_FILE"):
        return FileKeyProvider(Path(env["PGW_SIGNING_KEYS_FILE"]))
    if env.get("PGW_SIGNING_KEYS_SECRET"):
        return SecretManagerKeyProvider(env["PGW_SIGNING_KEYS_SECRET"])
    return None


@dataclass
class KeyRing:
    token_ttl_s: int = 900
    leeway_s: int = 60
    clock: Callable[[], float] = time.time
    keys: list[SigningKey] = field(default_factory=list)
    provider: KeyProvider | None = None

    def __post_init__(self) -> None:
        if not self.keys and self.provider is None:
            self.keys.append(generate_key("active", now=self.clock()))

    @classmethod
    async def from_provider(cls, provider: KeyProvider, **kwargs: Any) -> KeyRing:
        ring = cls(provider=provider, **kwargs)
        await ring.refresh()
        return ring

    async def refresh(self) -> bool:
        """Reload keys from the provider. Returns True if the active kid or the published set changed.

        A failed reload keeps the current keys: a Secret Manager blip must not stop token issuance."""
        if self.provider is None:
            return False
        try:
            keys = (await self.provider.load()).signing_keys()
        except Exception:
            if not self.keys:
                raise
            log.exception("signing key refresh failed; keeping the current keys", extra={"provider": self.provider.name})
            return False
        before = (self.active.kid if self.keys else None, {k.kid for k in self.keys})
        self.keys = keys
        changed = before != (self.active.kid, {k.kid for k in keys})
        if changed:
            log.info("signing keys loaded", extra={"active_kid": self.active.kid, "kids": sorted(k.kid for k in keys)})
        return changed

    @property
    def active(self) -> SigningKey:
        return next(k for k in self.keys if k.status == "active")

    def stage(self) -> SigningKey:
        staged = next((k for k in self.keys if k.status == "staged"), None)
        if staged is None:
            staged = generate_key("staged", now=self.clock())
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
        gone = [k.kid for k in self.keys if k.status == "retiring" and k.retire_after is not None
                and k.retire_after <= now]
        self.keys = [k for k in self.keys if k.kid not in gone]
        return gone

    def verification_key(self, kid: str) -> ec.EllipticCurvePublicKey | None:
        self.prune()
        k = next((k for k in self.keys if k.kid == kid), None)
        return k.private.public_key() if k else None

    def jwks(self) -> dict[str, list[dict[str, str]]]:
        self.prune()
        return {"keys": [k.public_jwk() for k in self.keys]}
