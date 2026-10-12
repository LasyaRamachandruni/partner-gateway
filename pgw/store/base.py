"""Interfaces for the gateway's state.

Every piece of state the gateway keeps sits behind one of these interfaces, so a
gateway replica holds nothing that another replica needs:

| State | Interface | Shared implementation |
|---|---|---|
| Rate-limit buckets | `RateLimitStore` | Redis (atomic Lua) |
| Idempotency keys | `IdempotencyStore` | Redis (atomic Lua) |
| Partners | `PartnerRepository` | Postgres |
| Commands | `CommandRepository` | Postgres |
| Webhook deliveries (outbox) | `DeliveryRepository` | Postgres |

The in-memory implementations in `pgw.store.memory` keep unit tests and the
single-process sandbox free of infrastructure.

State changes that must happen once across replicas are single conditional writes
in the store: a command moves from `pending` to a final state only if it is still
pending, and a webhook delivery is claimed with a lease, so two replicas never send
the same attempt.
"""

from __future__ import annotations

from typing import Protocol

from ..resilience.idempotency import IdempotencyStore
from ..resilience.ratelimit import RateLimitStore
from .records import Command, Delivery, Partner

__all__ = [
    "CommandRepository",
    "DeliveryRepository",
    "IdempotencyStore",
    "PartnerRepository",
    "RateLimitStore",
    "Stores",
]


class PartnerRepository(Protocol):
    async def add(self, partner: Partner) -> None: ...

    async def by_client_id(self, client_id: str) -> Partner | None: ...

    async def by_id(self, partner_id: str) -> Partner | None: ...

    async def save(self, partner: Partner) -> None:
        """Persist changes to an existing partner (secrets, grants, enabled)."""
        ...


class CommandRepository(Protocol):
    async def add(self, command: Command) -> None: ...

    async def get(self, command_id: str) -> Command | None: ...

    async def delete(self, command_id: str) -> None: ...

    async def complete(self, command_id: str, status: str, reason: str | None, at: float) -> Command | None:
        """Move a pending command to a final state. Returns it, or None if it was unknown or already final."""
        ...

    async def expire_pending(self, created_before: float, reason: str, at: float) -> list[Command]:
        """Expire every command still pending that was created before `created_before`."""
        ...


class DeliveryRepository(Protocol):
    async def add(self, delivery: Delivery) -> None: ...

    async def get(self, event_id: str) -> Delivery | None: ...

    async def list_for_partner(self, partner_id: str, status: str | None = None, limit: int = 100) -> list[Delivery]:
        """The partner's most recent deliveries, oldest first."""
        ...

    async def claim_due(self, now: float, limit: int, lease_s: float) -> list[Delivery]:
        """Pending deliveries whose next attempt is due, leased to the caller for `lease_s`.

        A delivery leased by one replica isn't returned to another until the lease runs out."""
        ...

    async def save(self, delivery: Delivery) -> None:
        """Store the outcome of an attempt (or a replay) and release the lease."""
        ...

    async def count_pending(self) -> int: ...


class Stores(Protocol):
    partners: PartnerRepository
    commands: CommandRepository
    deliveries: DeliveryRepository
    ratelimits: RateLimitStore
    idempotency: IdempotencyStore

    async def ping(self) -> None:
        """Raise if a backing store is unreachable (used by /readyz)."""
        ...

    async def close(self) -> None: ...
