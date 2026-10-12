"""Command lifecycle on the gateway side.

    pending ──(result event)──► succeeded | failed | expired
       └──(no result within ttl)──────────► expired   (backstop if an event is lost)

Results arrive at least once and possibly out of order, and with several gateway
replicas, two replicas can receive the same result. The transition out of
`pending` is one conditional write in the command repository, so exactly one
of them applies it; the others see "already final" and ignore the event. Only
the replica that applied it sends the partner its `command.completed` webhook.

The expiry sweep runs on every replica. It is the same conditional write, so
each overdue command is expired (and its webhook sent) once.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid

from ..auth.partners import PartnerRegistry
from ..bus.base import Message
from ..obs import tracing
from ..obs.metrics import Metrics
from ..store.base import CommandRepository
from ..store.records import FINAL_COMMAND_STATES, Command
from .webhooks import WebhookDispatcher

log = logging.getLogger(__name__)

__all__ = ["Command", "CommandService"]


class CommandService:
    def __init__(
        self,
        repo: CommandRepository,
        partners: PartnerRegistry,
        webhooks: WebhookDispatcher,
        metrics: Metrics,
        ttl_s: float = 120.0,
    ) -> None:
        self.repo = repo
        self.partners = partners
        self.webhooks = webhooks
        self.metrics = metrics
        self.ttl_s = ttl_s
        self.duplicates_ignored = 0  # on this replica
        self._sweeper: asyncio.Task[None] | None = None

    async def create(self, partner_id: str, vin: str, type_: str) -> Command:
        c = Command(f"cmd_{uuid.uuid4().hex[:16]}", partner_id, vin, type_, "pending", time.time())
        await self.repo.add(c)
        return c

    async def discard(self, command_id: str) -> None:
        """The vehicle service never accepted the command: forget it."""
        await self.repo.delete(command_id)

    async def get(self, command_id: str, partner_id: str) -> Command | None:
        c = await self.repo.get(command_id)
        return c if c and c.partner_id == partner_id else None  # other partners' commands don't exist

    async def on_result(self, msg: Message) -> None:
        with tracing.tracer("partner-gateway").start_as_current_span(
            "command.result", context=tracing.extract(msg.attributes)
        ):
            data = msg.data
            status = str(data.get("status", "")).lower()
            if status not in FINAL_COMMAND_STATES:
                self.metrics.events.labels(msg.topic, "invalid").inc()
                log.warning(
                    "ignoring a result with an unknown status",
                    extra={"command_id": data.get("command_id"), "status": status},
                )
                return
            c = await self.repo.complete(str(data.get("command_id", "")), status, data.get("reason"), time.time())
            if c is None:
                known = await self.repo.get(str(data.get("command_id", "")))
                outcome = "duplicate" if known else "unknown_command"
                if known:
                    self.duplicates_ignored += 1
                self.metrics.events.labels(msg.topic, outcome).inc()
                return
            await self._finished(c)
            self.metrics.events.labels(msg.topic, "applied").inc()

    async def _finished(self, c: Command) -> None:
        self.metrics.commands.labels(c.type, c.status).inc()
        if c.completed_at is not None:
            self.metrics.command_seconds.observe(c.completed_at - c.created_at)
        partner = await self.partners.by_id(c.partner_id)
        if partner:
            await self.webhooks.enqueue(partner, "command.completed", c.public())

    async def sweep(self, now: float | None = None) -> int:
        now = now or time.time()
        expired = await self.repo.expire_pending(now - self.ttl_s, "no result from the vehicle service in time", now)
        for c in expired:
            await self._finished(c)
        if expired:
            log.info("expired commands with no result", extra={"count": len(expired)})
        return len(expired)

    def start(self, every_s: float = 5.0) -> None:
        async def loop() -> None:
            while True:
                await asyncio.sleep(every_s)
                try:
                    await self.sweep()
                except Exception:
                    log.exception("command sweep failed")

        self._sweeper = asyncio.create_task(loop(), name="command-sweeper")

    async def stop(self) -> None:
        if self._sweeper:
            self._sweeper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._sweeper
