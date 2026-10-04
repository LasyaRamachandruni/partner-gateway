"""Verify webhook signatures from the partner gateway.

    from partner_sdk import verify_signature
    verify_signature(request.body, request.headers["Pgw-Signature"], secret)   # raises InvalidSignature

Checks, in order:
1. The header has a timestamp and at least one v1 signature.
2. The timestamp is within `tolerance_s` (5 minutes) of now. This rejects a
   captured delivery replayed later.
3. Some v1 signature equals HMAC-SHA256(secret, "<t>.<raw body>"), compared in
   constant time. Use the raw request bytes: re-serializing the JSON changes them.

During a secret rotation, pass both secrets (old and new).
"""

from __future__ import annotations

import hashlib
import hmac
import time


class InvalidSignature(Exception):
    pass


def verify_signature(body: bytes, header: str, secrets: str | list[str], *, tolerance_s: int = 300,
                     now: float | None = None) -> int:
    """Returns the signed timestamp. Raises InvalidSignature."""
    secrets = [secrets] if isinstance(secrets, str) else secrets
    parts = [p.split("=", 1) for p in header.split(",") if "=" in p]
    ts_values = [v for k, v in parts if k == "t"]
    sigs = [v for k, v in parts if k == "v1"]
    if len(ts_values) != 1 or not sigs:
        raise InvalidSignature("malformed signature header")
    try:
        ts = int(ts_values[0])
    except ValueError as exc:
        raise InvalidSignature("bad timestamp") from exc
    if abs((now or time.time()) - ts) > tolerance_s:
        raise InvalidSignature("timestamp outside tolerance (possible replay)")
    for secret in secrets:
        expected = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
        if any(hmac.compare_digest(expected, s) for s in sigs):
            return ts
    raise InvalidSignature("no matching signature")
