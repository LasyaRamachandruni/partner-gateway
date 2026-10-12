"""Signed, retried webhook delivery to partners, from a shared outbox.

Every event is POSTed as JSON to the partner's webhook URL with:

  Pgw-Event-Id: evt_...       (the same id on every retry, so partners can deduplicate)
  Pgw-Signature: t=<unix>,v1=<hex hmac>[,v1=<hex hmac>]
  traceparent: ...

The signature is HMAC-SHA256 over "<t>.<raw body>" with the partner's webhook
secret. During secret rotation there is one `v1` per active secret. Partners
verify with `partner_sdk.webhooks.verify_signature`, which also rejects old
timestamps, so a captured delivery can't be replayed later.

Deliveries are rows in a `DeliveryRepository` (the outbox), not tasks in one
process. `enqueue` writes a row; a worker on every replica claims due rows with a
lease, attempts them, and writes back the outcome and the next attempt time. A
replica that dies mid-attempt loses nothing: its lease runs out and another
replica picks the delivery up (the partner may then see the event twice, which
the event id lets it ignore).

Delivery is retried with backoff on network errors, timeouts, 429 and 5xx.
A 4xx (other than 408/429) means the partner rejected it, so it isn't retried.
After `max_attempts`, the delivery goes to the dead-letter list, where it can be
inspected and replayed through the API once the partner has fixed their endpoint.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import random
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

from ..obs import tracing
from ..obs.metrics import Metrics
from ..store.base import DeliveryRepository
from ..store.memory import InMemoryDeliveries
from ..store.records import Delivery, Partner

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}

PartnerLookup = Callable[[str], Awaitable[Partner | None]]


def sign(body: bytes, secret: str, timestamp: int) -> str:
    return hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()


def signature_header(body: bytes, secrets: list[str], timestamp: int) -> str:
    return ",".join([f"t={timestamp}"] + [f"v1={sign(body, s, timestamp)}" for s in secrets])


class WebhookDispatcher:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        repo: DeliveryRepository | None = None,
        backoff_s: tuple[float, ...] = (0.5, 2, 10, 30, 120),
        timeout_s: float = 5.0,
        metrics: Metrics | None = None,
        seed: int | None = None,
        poll_s: float = 1.0,
        batch: int = 50,
        lease_s: float = 30.0,
        concurrency: int = 20,
    ) -> None:
        self.client = client
        self.repo: DeliveryRepository = repo or InMemoryDeliveries()
        self.backoff_s = backoff_s
        self.max_attempts = len(backoff_s) + 1
        self.timeout_s = timeout_s
        self.metrics = metrics or Metrics()
        self.rng = random.Random(seed)
        self.poll_s = poll_s
        self.batch = batch
        self.lease_s = lease_s
        self._sem = asyncio.Semaphore(concurrency)
        self._wake = asyncio.Event()
        self._worker: asyncio.Task[None] | None = None
        self.partner_lookup: PartnerLookup | None = None  # set by the gateway: current secrets at send time

    def start(self) -> None:
        self._worker = asyncio.create_task(self._run(), name="webhooks")

    async def stop(self) -> None:
        if self._worker:
            self._worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._worker

    async def enqueue(self, partner: Partner, event_type: str, data: dict[str, Any]) -> Delivery | None:
        if not partner.webhook_url:
            return None
        eid = f"evt_{uuid.uuid4().hex[:16]}"
        now = time.time()
        event = {"id": eid, "type": event_type, "created_at": int(now), "data": data}
        d = Delivery(
            eid, partner.partner_id, partner.webhook_url, event, tracing.inject(), next_attempt_at=now, created_at=now
        )
        await self.repo.add(d)
        self._wake.set()
        return d

    async def replay(self, event_id: str) -> Delivery:
        d = await self.repo.get(event_id)
        if d is None:
            raise KeyError(event_id)
        d.status, d.attempts, d.last_error, d.next_attempt_at = "pending", 0, None, time.time()
        await self.repo.save(d)
        self._wake.set()
        return d

    async def _run(self) -> None:
        while True:
            try:
                claimed = await self.repo.claim_due(time.time(), self.batch, self.lease_s)
            except Exception:
                log.exception("claiming webhook deliveries failed")
                claimed = []
            if claimed:
                await asyncio.gather(*(self._guarded(d) for d in claimed))
                if len(claimed) == self.batch:
                    continue  # more may be due right now
            self._wake.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=self.poll_s)

    async def _guarded(self, d: Delivery) -> None:
        async with self._sem:
            try:
                await self._attempt(d)
            except Exception:
                log.exception("webhook attempt crashed", extra={"event_id": d.event_id})

    async def _attempt(self, d: Delivery) -> None:
        partner = await self.partner_lookup(d.partner_id) if self.partner_lookup else None
        if partner is None or not partner.webhook_secrets:
            d.status, d.last_error = "dead", "partner or webhook secret no longer exists"
            await self.repo.save(d)
            self.metrics.webhooks.labels("dead_lettered").inc()
            return
        d.attempts += 1
        body = json.dumps(d.event, separators=(",", ":")).encode()
        ts = int(time.time())
        headers = {
            "Content-Type": "application/json",
            "Pgw-Event-Id": d.event_id,
            "Pgw-Signature": signature_header(body, partner.webhook_secrets, ts),
            **d.trace,
        }
        retryable, error = True, None
        try:
            with tracing.tracer("partner-gateway").start_as_current_span(
                "webhook.deliver",
                context=tracing.extract(d.trace),
                attributes={"partner": d.partner_id, "attempt": d.attempts},
            ):
                r = await self.client.post(d.url, content=body, headers=headers, timeout=self.timeout_s)
            if 200 <= r.status_code < 300:
                d.status = "delivered"
                d.history.append({"attempt": d.attempts, "status": r.status_code})
                await self.repo.save(d)
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
            await self.repo.save(d)
            self.metrics.webhooks.labels("dead_lettered").inc()
            log.warning(
                "webhook dead-lettered", extra={"event_id": d.event_id, "partner_id": d.partner_id, "error": error}
            )
            return
        self.metrics.webhooks.labels("retried").inc()
        d.next_attempt_at = time.time() + self.backoff_s[d.attempts - 1] * self.rng.uniform(0.8, 1.2)
        await self.repo.save(d)

    async def drain(self, timeout: float = 5.0) -> None:
        """Test helper: wait until no delivery is pending."""
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if await self.repo.count_pending() == 0:
                return
            self._wake.set()
            await asyncio.sleep(0.01)
        raise TimeoutError("webhooks still pending")
