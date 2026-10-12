"""The records the gateway keeps: partners, commands and webhook deliveries.

These are plain dataclasses. Where they live (process memory, Postgres) is decided
by the repositories in `pgw.store`.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

SCOPES = frozenset({"vehicles:read", "vehicles:command"})
FINAL_COMMAND_STATES = frozenset({"succeeded", "failed", "expired"})


@dataclass
class Partner:
    partner_id: str
    name: str
    client_id: str
    secret_hash: str
    scopes: set[str]
    vins: set[str]
    rate_per_s: float = 5.0
    burst: int = 10
    webhook_url: str | None = None
    webhook_secrets: list[str] = field(default_factory=list)
    enabled: bool = True


@dataclass
class Command:
    command_id: str
    partner_id: str
    vin: str
    type: str
    status: str  # pending | succeeded | failed | expired
    created_at: float
    completed_at: float | None = None
    reason: str | None = None

    def public(self) -> dict[str, Any]:
        d = asdict(self)
        d.pop("partner_id")
        return d


@dataclass
class Delivery:
    event_id: str
    partner_id: str
    url: str
    event: dict[str, Any]
    trace: dict[str, str]
    attempts: int = 0
    status: str = "pending"  # pending | delivered | dead
    last_error: str | None = None
    next_attempt_at: float = 0.0
    created_at: float = 0.0
    history: list[dict[str, Any]] = field(default_factory=list)
