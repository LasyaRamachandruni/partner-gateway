"""Postgres implementations of the durable records: partners, commands, webhook deliveries.

SQLAlchemy 2 (async, asyncpg). The schema is managed by Alembic (`pgw db upgrade`,
migrations in `pgw/migrations`). The operations that must happen once across
replicas are single statements:

- `complete`:  UPDATE commands ... WHERE command_id = :id AND status = 'pending' RETURNING ...
- `expire_pending`: the same, for every overdue pending command
- `claim_due`: UPDATE ... WHERE event_id IN (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING ...,
  which leases due webhook deliveries to one replica without blocking the others.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    delete,
    func,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .records import FINAL_COMMAND_STATES, Command, Delivery, Partner

Json = JSON().with_variant(JSONB(), "postgresql")


def _ts(epoch: float | None) -> datetime | None:
    return None if epoch is None else datetime.fromtimestamp(epoch, UTC)


def _epoch(ts: datetime | None) -> float | None:
    return None if ts is None else ts.timestamp()


class Base(DeclarativeBase):
    pass


class PartnerRow(Base):
    __tablename__ = "partners"

    partner_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(200))
    client_id: Mapped[str] = mapped_column(String(64), unique=True)
    secret_hash: Mapped[str] = mapped_column(String(200))
    scopes: Mapped[list[str]] = mapped_column(Json)
    vins: Mapped[list[str]] = mapped_column(Json)
    rate_per_s: Mapped[float] = mapped_column(Float)
    burst: Mapped[int] = mapped_column(Integer)
    webhook_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    webhook_secrets: Mapped[list[str]] = mapped_column(Json)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    def to_record(self) -> Partner:
        return Partner(self.partner_id, self.name, self.client_id, self.secret_hash, set(self.scopes), set(self.vins),
                       self.rate_per_s, self.burst, self.webhook_url, list(self.webhook_secrets), self.enabled)


class CommandRow(Base):
    __tablename__ = "commands"
    __table_args__ = (
        Index("ix_commands_pending_created", "created_at", postgresql_where=text("status = 'pending'")),
        Index("ix_commands_partner", "partner_id"),
    )

    command_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    partner_id: Mapped[str] = mapped_column(String(64))
    vin: Mapped[str] = mapped_column(String(32))
    type: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(16))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    def to_record(self) -> Command:
        return Command(self.command_id, self.partner_id, self.vin, self.type, self.status,
                       self.created_at.timestamp(), _epoch(self.completed_at), self.reason)


class DeliveryRow(Base):
    __tablename__ = "webhook_deliveries"
    __table_args__ = (
        Index("ix_deliveries_due", "next_attempt_at", postgresql_where=text("status = 'pending'")),
        Index("ix_deliveries_partner_created", "partner_id", "created_at"),
    )

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    partner_id: Mapped[str] = mapped_column(String(64))
    url: Mapped[str] = mapped_column(Text)
    event: Mapped[dict[str, Any]] = mapped_column(Json)
    trace: Mapped[dict[str, str]] = mapped_column(Json)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(16))
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    history: Mapped[list[dict[str, Any]]] = mapped_column(Json)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    def to_record(self) -> Delivery:
        return Delivery(self.event_id, self.partner_id, self.url, dict(self.event), dict(self.trace), self.attempts,
                        self.status, self.last_error, self.next_attempt_at.timestamp(), self.created_at.timestamp(),
                        list(self.history))


def make_engine(url: str, *, pool_size: int = 10, echo: bool = False) -> AsyncEngine:
    """An async engine. `url` is a SQLAlchemy URL, e.g. postgresql+asyncpg://user:pw@host/db
    (on Cloud Run: postgresql+asyncpg://user:pw@/db?host=/cloudsql/PROJECT:REGION:INSTANCE)."""
    return create_async_engine(url, pool_size=pool_size, max_overflow=pool_size, pool_pre_ping=True, echo=echo)


class SqlPartners:
    def __init__(self, sessions: async_sessionmaker[Any]) -> None:
        self.sessions = sessions

    async def add(self, partner: Partner) -> None:
        async with self.sessions.begin() as s:
            s.add(PartnerRow(partner_id=partner.partner_id, name=partner.name, client_id=partner.client_id,
                             secret_hash=partner.secret_hash, scopes=sorted(partner.scopes), vins=sorted(partner.vins),
                             rate_per_s=partner.rate_per_s, burst=partner.burst, webhook_url=partner.webhook_url,
                             webhook_secrets=list(partner.webhook_secrets), enabled=partner.enabled))

    async def by_client_id(self, client_id: str) -> Partner | None:
        async with self.sessions() as s:
            row = (await s.execute(select(PartnerRow).where(PartnerRow.client_id == client_id))).scalar_one_or_none()
            return row.to_record() if row else None

    async def by_id(self, partner_id: str) -> Partner | None:
        async with self.sessions() as s:
            row = await s.get(PartnerRow, partner_id)
            return row.to_record() if row else None

    async def save(self, partner: Partner) -> None:
        async with self.sessions.begin() as s:
            result = await s.execute(
                update(PartnerRow).where(PartnerRow.partner_id == partner.partner_id).values(
                    name=partner.name, scopes=sorted(partner.scopes), vins=sorted(partner.vins),
                    rate_per_s=partner.rate_per_s, burst=partner.burst, webhook_url=partner.webhook_url,
                    webhook_secrets=list(partner.webhook_secrets), enabled=partner.enabled))
            if result.rowcount == 0:  # type: ignore[attr-defined]
                raise KeyError(partner.partner_id)


class SqlCommands:
    def __init__(self, sessions: async_sessionmaker[Any]) -> None:
        self.sessions = sessions

    async def add(self, command: Command) -> None:
        async with self.sessions.begin() as s:
            s.add(CommandRow(command_id=command.command_id, partner_id=command.partner_id, vin=command.vin,
                             type=command.type, status=command.status, created_at=_ts(command.created_at),
                             completed_at=_ts(command.completed_at), reason=command.reason))

    async def get(self, command_id: str) -> Command | None:
        async with self.sessions() as s:
            row = await s.get(CommandRow, command_id)
            return row.to_record() if row else None

    async def delete(self, command_id: str) -> None:
        async with self.sessions.begin() as s:
            await s.execute(delete(CommandRow).where(CommandRow.command_id == command_id))

    async def complete(self, command_id: str, status: str, reason: str | None, at: float) -> Command | None:
        if status not in FINAL_COMMAND_STATES:
            raise ValueError(f"not a final state: {status}")
        async with self.sessions.begin() as s:
            row = (await s.execute(
                update(CommandRow)
                .where(CommandRow.command_id == command_id, CommandRow.status == "pending")
                .values(status=status, reason=reason, completed_at=_ts(at))
                .returning(CommandRow)
            )).scalar_one_or_none()
            return row.to_record() if row else None

    async def expire_pending(self, created_before: float, reason: str, at: float) -> list[Command]:
        async with self.sessions.begin() as s:
            rows = (await s.execute(
                update(CommandRow)
                .where(CommandRow.status == "pending", CommandRow.created_at < _ts(created_before))
                .values(status="expired", reason=reason, completed_at=_ts(at))
                .returning(CommandRow)
            )).scalars().all()
            return [r.to_record() for r in rows]


class SqlDeliveries:
    def __init__(self, sessions: async_sessionmaker[Any]) -> None:
        self.sessions = sessions

    async def add(self, delivery: Delivery) -> None:
        async with self.sessions.begin() as s:
            s.add(DeliveryRow(event_id=delivery.event_id, partner_id=delivery.partner_id, url=delivery.url,
                              event=delivery.event, trace=delivery.trace, attempts=delivery.attempts,
                              status=delivery.status, last_error=delivery.last_error,
                              next_attempt_at=_ts(delivery.next_attempt_at), locked_until=None,
                              history=delivery.history, created_at=_ts(delivery.created_at)))

    async def get(self, event_id: str) -> Delivery | None:
        async with self.sessions() as s:
            row = await s.get(DeliveryRow, event_id)
            return row.to_record() if row else None

    async def list_for_partner(self, partner_id: str, status: str | None = None, limit: int = 100) -> list[Delivery]:
        q = select(DeliveryRow).where(DeliveryRow.partner_id == partner_id)
        if status is not None:
            q = q.where(DeliveryRow.status == status)
        q = q.order_by(DeliveryRow.created_at.desc()).limit(limit)
        async with self.sessions() as s:
            rows = (await s.execute(q)).scalars().all()
            return [r.to_record() for r in reversed(rows)]

    async def claim_due(self, now: float, limit: int, lease_s: float) -> list[Delivery]:
        now_ts = _ts(now)
        due = (
            select(DeliveryRow.event_id)
            .where(DeliveryRow.status == "pending", DeliveryRow.next_attempt_at <= now_ts,
                   (DeliveryRow.locked_until.is_(None)) | (DeliveryRow.locked_until <= now_ts))
            .order_by(DeliveryRow.next_attempt_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
            .scalar_subquery()
        )
        async with self.sessions.begin() as s:
            rows = (await s.execute(
                update(DeliveryRow)
                .where(DeliveryRow.event_id.in_(due))
                .values(locked_until=now_ts + timedelta(seconds=lease_s) if now_ts else None)
                .returning(DeliveryRow)
            )).scalars().all()
            return sorted((r.to_record() for r in rows), key=lambda d: d.next_attempt_at)

    async def save(self, delivery: Delivery) -> None:
        async with self.sessions.begin() as s:
            await s.execute(
                update(DeliveryRow).where(DeliveryRow.event_id == delivery.event_id).values(
                    attempts=delivery.attempts, status=delivery.status, last_error=delivery.last_error,
                    next_attempt_at=_ts(delivery.next_attempt_at), locked_until=None, history=delivery.history))

    async def count_pending(self) -> int:
        async with self.sessions() as s:
            n = await s.scalar(select(func.count()).select_from(DeliveryRow).where(DeliveryRow.status == "pending"))
            return int(n or 0)
