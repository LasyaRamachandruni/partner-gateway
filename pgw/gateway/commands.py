"""Command lifecycle on the gateway side.

    pending ──(result event)──► succeeded | failed | expired
       └──(no result within ttl)──────────► expired   (backstop if an event is lost)

Results arrive at least once and possibly out of order. A result for a command
that is already final is ignored, so applying an event twice changes nothing.
When a command reaches a final state, the partner gets a `command.completed` webhook.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import asdict, dataclass

from ..auth.partners import PartnerRegistry
from ..bus.base import Message
from ..obs import tracing
from ..obs.metrics import Metrics
from .webhooks import WebhookDispatcher

FINAL = {"succeeded", "failed", "expired"}


@dataclass
class Command:
    command_id: str
    partner_id: str
    vin: str
    type: str
    status: str
    created_at: float
    completed_at: float | None = None
    reason: str | None = None

    def public(self) -> dict:
        d = asdict(self)
        d.pop("partner_id")
        return d


class CommandStore:
    def __init__(self, partners: PartnerRegistry, webhooks: WebhookDispatcher, metrics: Metrics,
                 ttl_s: float = 120.0):
        self.partners = partners
        self.webhooks = webhooks
        self.metrics = metrics
        self.ttl_s = ttl_s
        self.commands: dict[str, Command] = {}
        self.duplicates_ignored = 0
        self._sweeper: asyncio.Task | None = None

    def create(self, partner_id: str, vin: str, type_: str) -> Command:
        c = Command(f"cmd_{uuid.uuid4().hex[:16]}", partner_id, vin, type_, "pending", time.time())
        self.commands[c.command_id] = c
        return c

    def get(self, command_id: str, partner_id: str) -> Command | None:
        c = self.commands.get(command_id)
        return c if c and c.partner_id == partner_id else None  # other partners' commands don't exist

    async def on_result(self, msg: Message) -> None:
        with tracing.tracer("partner-gateway").start_as_current_span(
                "command.result", context=tracing.extract(msg.attributes)):
            data = msg.data
            c = self.commands.get(data.get("command_id", ""))
            if c is None:
                self.metrics.events.labels(msg.topic, "unknown_command").inc()
                return
            if c.status in FINAL:
                self.duplicates_ignored += 1
                self.metrics.events.labels(msg.topic, "duplicate").inc()
                return
            self._finish(c, data["status"].lower(), data.get("reason"))
            self.metrics.events.labels(msg.topic, "applied").inc()

    def _finish(self, c: Command, status: str, reason: str | None) -> None:
        c.status, c.reason, c.completed_at = status, reason, time.time()
        self.metrics.commands.labels(c.type, status).inc()
        self.metrics.command_seconds.observe(c.completed_at - c.created_at)
        partner = self.partners.by_id(c.partner_id)
        if partner:
            self.webhooks.enqueue(partner, "command.completed", c.public())

    def sweep(self, now: float | None = None) -> int:
        now = now or time.time()
        expired = [c for c in self.commands.values() if c.status == "pending" and now - c.created_at > self.ttl_s]
        for c in expired:
            self._finish(c, "expired", "no result from the vehicle service in time")
        return len(expired)

    def start(self, every_s: float = 5.0) -> None:
        async def loop():
            while True:
                await asyncio.sleep(every_s)
                self.sweep()

        self._sweeper = asyncio.create_task(loop(), name="command-sweeper")

    async def stop(self) -> None:
        if self._sweeper:
            self._sweeper.cancel()
            await asyncio.gather(self._sweeper, return_exceptions=True)
