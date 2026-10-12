"""In-process implementations of the store interfaces.

For unit tests and the single-process sandbox. They follow the same rules as the
Redis and Postgres implementations (conditional completion, leased claims), which
the shared contract tests in `tests/test_stores.py` check against all of them.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field

from ..resilience.idempotency import InMemoryIdempotencyStore
from ..resilience.ratelimit import InMemoryRateLimiter
from .records import FINAL_COMMAND_STATES, Command, Delivery, Partner


class InMemoryPartners:
    def __init__(self) -> None:
        self._by_id: dict[str, Partner] = {}

    async def add(self, partner: Partner) -> None:
        if any(p.client_id == partner.client_id for p in self._by_id.values()):
            raise ValueError(f"client id {partner.client_id} already exists")
        self._by_id[partner.partner_id] = copy.deepcopy(partner)

    async def by_client_id(self, client_id: str) -> Partner | None:
        p = next((p for p in self._by_id.values() if p.client_id == client_id), None)
        return copy.deepcopy(p) if p else None

    async def by_id(self, partner_id: str) -> Partner | None:
        p = self._by_id.get(partner_id)
        return copy.deepcopy(p) if p else None

    async def save(self, partner: Partner) -> None:
        if partner.partner_id not in self._by_id:
            raise KeyError(partner.partner_id)
        self._by_id[partner.partner_id] = copy.deepcopy(partner)


class InMemoryCommands:
    def __init__(self) -> None:
        self._items: dict[str, Command] = {}

    async def add(self, command: Command) -> None:
        self._items[command.command_id] = copy.copy(command)

    async def get(self, command_id: str) -> Command | None:
        c = self._items.get(command_id)
        return copy.copy(c) if c else None

    async def delete(self, command_id: str) -> None:
        self._items.pop(command_id, None)

    async def complete(self, command_id: str, status: str, reason: str | None, at: float) -> Command | None:
        if status not in FINAL_COMMAND_STATES:
            raise ValueError(f"not a final state: {status}")
        c = self._items.get(command_id)
        if c is None or c.status != "pending":
            return None
        c.status, c.reason, c.completed_at = status, reason, at
        return copy.copy(c)

    async def expire_pending(self, created_before: float, reason: str, at: float) -> list[Command]:
        out = []
        for c in self._items.values():
            if c.status == "pending" and c.created_at < created_before:
                c.status, c.reason, c.completed_at = "expired", reason, at
                out.append(copy.copy(c))
        return out


class InMemoryDeliveries:
    def __init__(self) -> None:
        self._items: dict[str, Delivery] = {}
        self._leases: dict[str, float] = {}

    async def add(self, delivery: Delivery) -> None:
        self._items[delivery.event_id] = copy.deepcopy(delivery)

    async def get(self, event_id: str) -> Delivery | None:
        d = self._items.get(event_id)
        return copy.deepcopy(d) if d else None

    async def list_for_partner(self, partner_id: str, status: str | None = None, limit: int = 100) -> list[Delivery]:
        items = [d for d in self._items.values() if d.partner_id == partner_id and status in (None, d.status)]
        items.sort(key=lambda d: d.created_at)
        return [copy.deepcopy(d) for d in items[-limit:]]

    async def claim_due(self, now: float, limit: int, lease_s: float) -> list[Delivery]:
        due = sorted(
            (d for d in self._items.values()
             if d.status == "pending" and d.next_attempt_at <= now and self._leases.get(d.event_id, 0.0) <= now),
            key=lambda d: d.next_attempt_at,
        )[:limit]
        for d in due:
            self._leases[d.event_id] = now + lease_s
        return [copy.deepcopy(d) for d in due]

    async def save(self, delivery: Delivery) -> None:
        self._items[delivery.event_id] = copy.deepcopy(delivery)
        self._leases.pop(delivery.event_id, None)

    async def count_pending(self) -> int:
        return sum(1 for d in self._items.values() if d.status == "pending")

    def all(self) -> list[Delivery]:
        """Test helper."""
        return [copy.deepcopy(d) for d in self._items.values()]


@dataclass
class InMemoryStores:
    partners: InMemoryPartners = field(default_factory=InMemoryPartners)
    commands: InMemoryCommands = field(default_factory=InMemoryCommands)
    deliveries: InMemoryDeliveries = field(default_factory=InMemoryDeliveries)
    ratelimits: InMemoryRateLimiter = field(default_factory=InMemoryRateLimiter)
    idempotency: InMemoryIdempotencyStore = field(default_factory=InMemoryIdempotencyStore)

    async def ping(self) -> None:
        return None

    async def close(self) -> None:
        return None
