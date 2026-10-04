"""Partner credentials, tokens and signing-key rotation."""

import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from pgw.auth.keys import KeyRing
from pgw.auth.partners import PartnerRegistry, check_secret, hash_secret
from pgw.auth.tokens import AUDIENCE, ISSUER, TokenError, TokenService


class Clock:
    def __init__(self, t=None):
        self.t = time.time() if t is None else t  # near real time: PyJWT checks nbf/exp against the system clock

    def __call__(self):
        return self.t


def test_secrets_are_hashed_and_checked():
    h = hash_secret("s3cret")
    assert "s3cret" not in h and check_secret("s3cret", h) and not check_secret("nope", h)


def test_authenticate_partner():
    reg = PartnerRegistry()
    p, secret = reg.register("Acme Insurance", {"vehicles:read"}, {"VIN1"})
    assert reg.authenticate(p.client_id, secret) is p
    assert reg.authenticate(p.client_id, "wrong") is None
    assert reg.authenticate("pc_unknown", secret) is None
    p.enabled = False
    assert reg.authenticate(p.client_id, secret) is None
    with pytest.raises(ValueError):
        reg.register("x", {"admin"}, set())


def test_token_round_trip_and_scope_narrowing():
    clock = Clock()
    svc = TokenService(KeyRing(clock=clock), clock=clock)
    p, _ = PartnerRegistry().register("Fleet Co", {"vehicles:read", "vehicles:command"}, {"VIN1"})
    token, ttl, scopes = svc.issue(p, {"vehicles:read"})
    assert scopes == {"vehicles:read"} and ttl == 900
    c = svc.verify(token)
    assert c.partner_id == p.partner_id and c.scopes == {"vehicles:read"}
    with pytest.raises(TokenError):
        svc.issue(p, {"something:else"})


def test_expired_tampered_and_unsigned_tokens_rejected():
    clock = Clock()
    ring = KeyRing(clock=clock)
    svc = TokenService(ring, clock=clock)
    p, _ = PartnerRegistry().register("x", {"vehicles:read"}, set())
    token, *_ = svc.issue(p)

    # expired (PyJWT checks exp against real time, so build one already expired)
    expired = jwt.encode({"iss": ISSUER, "aud": AUDIENCE, "sub": "p", "iat": 1, "exp": 2},
                         ring.active.private_pem(), algorithm="ES256", headers={"kid": ring.active.kid})
    with pytest.raises(TokenError, match="expired"):
        svc.verify(expired)
    # tampered payload
    head, body, sig = token.split(".")
    forged = jwt.utils.base64url_encode(b'{"sub":"someone-else"}').decode()
    with pytest.raises(TokenError):
        svc.verify(f"{head}.{forged}.{sig}")
    # alg=none
    none = jwt.encode({"sub": "p"}, None, algorithm="none", headers={"kid": ring.active.kid})
    with pytest.raises(TokenError):
        svc.verify(none)
    # signed by a key that isn't ours, with our kid
    other = ec.generate_private_key(ec.SECP256R1())
    fake = jwt.encode({"iss": ISSUER, "aud": AUDIENCE, "sub": "p", "iat": int(clock.t), "exp": int(clock.t) + 60},
                      other, algorithm="ES256", headers={"kid": ring.active.kid})
    with pytest.raises(TokenError):
        svc.verify(fake)


def test_key_rotation_keeps_old_tokens_valid_until_they_expire():
    clock = Clock()
    ring = KeyRing(token_ttl_s=900, leeway_s=60, clock=clock)
    svc = TokenService(ring, clock=clock)
    p, _ = PartnerRegistry().register("x", {"vehicles:read"}, set())
    old_token, *_ = svc.issue(p)
    old_kid = ring.active.kid

    staged = ring.stage()
    assert {k["kid"] for k in ring.jwks()["keys"]} == {old_kid, staged.kid}  # published before use
    assert ring.active.kid == old_kid

    ring.rotate()
    new_token, *_ = svc.issue(p)
    assert jwt.get_unverified_header(new_token)["kid"] == staged.kid
    assert svc.verify(old_token).partner_id == p.partner_id  # still valid after rotation

    clock.t += 961
    assert old_kid not in {k["kid"] for k in ring.jwks()["keys"]}  # pruned after ttl + leeway
    with pytest.raises(TokenError, match="unknown signing key"):
        svc.verify(old_token)


def test_jwks_has_no_private_material():
    jwk = KeyRing().jwks()["keys"][0]
    assert set(jwk) == {"kty", "crv", "kid", "use", "alg", "x", "y"}
