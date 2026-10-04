"""Signed, retried webhook delivery to partners.

Every event is POSTed as JSON to the partner's webhook URL with:

  Pgw-Event-Id: evt_...       (the same id on every retry, so partners can deduplicate)
  Pgw-Signature: t=<unix>,v1=<hex hmac>[,v1=<hex hmac>]
  traceparent: ...

The signature is HMAC-SHA256 over "<t>.<raw body>" with the partner's webhook
secret. During secret rotation there is one `v1` per active secret. Partners
verify with `partner_sdk.webhooks.verify_signature`, which also rejects old
timestamps, so a captured delivery can't be replayed later.

Delivery is retried with backoff on network errors, timeouts, 429 and 5xx.
A 4xx (other than 408/429) means the partner rejected it, so it isn't retried.
After `max_attempts`, the delivery goes to the dead-letter list, where it can be
inspected and replayed through the API once the partner has fixed their endpoint.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import random
import time
import uuid
from dataclasses import dataclass, field

import httpx

from ..auth.partners import Partner
from ..obs import tracing
from ..obs.metrics import Metrics

RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}


def sign(body: bytes, secret: str, timestamp: int) -> str:
    return hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()


def signature_header(body: bytes, secrets: list[str], timestamp: int) -> str:
    return ",".join([f"t={timestamp}"] + [f"v1={sign(body, s, timestamp)}" for s in secrets])


@dataclass
class Delivery:
    event_id: str
    partner_id: str
    url: str
    event: dict
    trace: dict
    attempts: int = 0
    status: str = "pending"  # pending | delivered | dead
    last_error: str | None = None
    history: list[dict] = field(default_factory=list)


class WebhookDispatcher:
    def __init__(self, client: httpx.AsyncClient, *, backoff_s: tuple[float, ...] = (0.5, 2, 10, 30, 120),
                 timeout_s: float = 5.0, metrics: Metrics | None = None, seed: int | None = None):
        self.client = client
        self.backoff_s = backoff_s
        self.max_attempts = len(backoff_s) + 1
        self.timeout_s = timeout_s
        self.metrics = metrics or Metrics()
        self.rng = random.Random(seed)
        self.deliveries: dict[str, Delivery] = {}
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._partners: dict[str, Partner] = {}
        self._worker: asyncio.Task | None = None
        self._pending_retries: set[asyncio.TimerHandle] = set()

    def start(self) -> None:
        self._worker = asyncio.create_task(self._run(), name="webhooks")

    async def stop(self) -> None:
        for h in self._pending_retries:
            h.cancel()
        if self._worker:
            self._worker.cancel()
            await asyncio.gather(self._worker, return_exceptions=True)

    def enqueue(self, partner: Partner, event_type: str, data: dict) -> Delivery | None:
        if not partner.webhook_url:
            return None
        eid = f"evt_{uuid.uuid4().hex[:16]}"
        event = {"id": eid, "type": event_type, "created_at": int(time.time()), "data": data}
        d = Delivery(eid, partner.partner_id, partner.webhook_url, event, tracing.inject())
        self.deliveries[eid] = d
        self._partners[partner.partner_id] = partner
        self._queue.put_nowait(eid)
        return d

    def replay(self, event_id: str) -> Delivery:
        d = self.deliveries[event_id]
        d.status, d.attempts, d.last_error = "pending", 0, None
        self._queue.put_nowait(event_id)
        return d

    async def _run(self) -> None:
        while True:
            eid = await self._queue.get()
            try:
                await self._attempt(self.deliveries[eid])
            finally:
                self._queue.task_done()

    async def _attempt(self, d: Delivery) -> None:
        partner = self._partners[d.partner_id]
        d.attempts += 1
        body = json.dumps(d.event, separators=(",", ":")).encode()
        ts = int(time.time())
        headers = {"Content-Type": "application/json", "Pgw-Event-Id": d.event_id,
                   "Pgw-Signature": signature_header(body, partner.webhook_secrets, ts), **d.trace}
        retryable, error = True, None
        try:
            with tracing.tracer("partner-gateway").start_as_current_span(
                    "webhook.deliver", context=tracing.extract(d.trace),
                    attributes={"partner": d.partner_id, "attempt": d.attempts}):
                r = await self.client.post(d.url, content=body, headers=headers, timeout=self.timeout_s)
            if 200 <= r.status_code < 300:
                d.status = "delivered"
                d.history.append({"attempt": d.attempts, "status": r.status_code})
                self.metrics.webhooks.labels("delivered").inc()
                self.metrics.webhook_attempts.observe(d.attempts)
                return
            error = f"HTTP {r.status_code}"
            retryable = r.status_code in RETRYABLE_STATUS
        except httpx.HTTPError as e:
            error = f"{type(e).__name__}: {e}"
        d.last_error = error
        d.history.append({"attempt": d.attempts, "error": error})
        if not retryable or d.attempts >= self.max_attempts:
            d.status = "dead"
            self.metrics.webhooks.labels("dead_lettered").inc()
            return
        self.metrics.webhooks.labels("retried").inc()
        delay = self.backoff_s[d.attempts - 1] * self.rng.uniform(0.8, 1.2)
        loop = asyncio.get_running_loop()
        handle = loop.call_later(delay, self._queue.put_nowait, d.event_id)
        self._pending_retries.add(handle)

    async def drain(self, timeout: float = 5.0) -> None:
        """Test helper: wait until no delivery is pending."""
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if all(d.status != "pending" for d in self.deliveries.values()):
                return
            await asyncio.sleep(0.01)
        raise TimeoutError("webhooks still pending")
