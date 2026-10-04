"""OAuth 2.0 client-credentials access tokens (JWT, ES256).

Partners exchange their client id and secret for a short-lived access token
(15 minutes by default) and send it as `Authorization: Bearer <token>`. The
token carries the partner id (`sub`) and the granted scopes, so the gateway can
authorize each request without a database lookup. Scopes are the partner's
allowed scopes, narrowed to what it asked for.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Callable

import jwt

from .keys import KeyRing
from .partners import Partner

ISSUER = "https://partner-gateway.example"
AUDIENCE = "partner-api"
LEEWAY_S = 30


class TokenError(Exception):
    pass


@dataclass
class Claims:
    partner_id: str
    client_id: str
    scopes: set[str]
    expires_at: int
    token_id: str


@dataclass
class TokenService:
    keys: KeyRing
    clock: Callable[[], float] = time.time

    def issue(self, partner: Partner, requested: set[str] | None = None) -> tuple[str, int, set[str]]:
        scopes = set(partner.scopes) if not requested else requested & partner.scopes
        if requested and not scopes:
            raise TokenError("invalid_scope")
        now = int(self.clock())
        key = self.keys.active
        claims = {"iss": ISSUER, "aud": AUDIENCE, "sub": partner.partner_id, "client_id": partner.client_id,
                  "scope": " ".join(sorted(scopes)), "iat": now, "nbf": now, "exp": now + self.keys.token_ttl_s,
                  "jti": uuid.uuid4().hex}
        token = jwt.encode(claims, key.private_pem(), algorithm="ES256", headers={"kid": key.kid})
        return token, self.keys.token_ttl_s, scopes

    def verify(self, token: str) -> Claims:
        try:
            kid = jwt.get_unverified_header(token).get("kid")
        except jwt.PyJWTError as exc:
            raise TokenError("malformed token") from exc
        key = self.keys.verification_key(kid) if kid else None
        if key is None:
            raise TokenError("unknown signing key")
        try:
            # algorithms is pinned: a token claiming "none" or HS256 is rejected
            c = jwt.decode(token, key, algorithms=["ES256"], audience=AUDIENCE, issuer=ISSUER, leeway=LEEWAY_S,
                           options={"require": ["exp", "iat", "sub", "aud", "iss"]})
        except jwt.ExpiredSignatureError as exc:
            raise TokenError("token expired") from exc
        except jwt.PyJWTError as exc:
            raise TokenError(f"invalid token: {exc}") from exc
        return Claims(c["sub"], c.get("client_id", ""), set(c.get("scope", "").split()), c["exp"], c["jti"])
