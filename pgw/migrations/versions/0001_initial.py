"""Partners, commands and webhook deliveries

Revision ID: 0001
Revises:
Create Date: 2026-10-12
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "commands",
        sa.Column("command_id", sa.String(length=64), nullable=False),
        sa.Column("partner_id", sa.String(length=64), nullable=False),
        sa.Column("vin", sa.String(length=32), nullable=False),
        sa.Column("type", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("command_id"),
    )
    op.create_index("ix_commands_partner", "commands", ["partner_id"], unique=False)
    op.create_index(
        "ix_commands_pending_created",
        "commands",
        ["created_at"],
        unique=False,
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_table(
        "partners",
        sa.Column("partner_id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("client_id", sa.String(length=64), nullable=False),
        sa.Column("secret_hash", sa.String(length=200), nullable=False),
        sa.Column("scopes", postgresql.JSONB(), nullable=False),
        sa.Column("vins", postgresql.JSONB(), nullable=False),
        sa.Column("rate_per_s", sa.Float(), nullable=False),
        sa.Column("burst", sa.Integer(), nullable=False),
        sa.Column("webhook_url", sa.Text(), nullable=True),
        sa.Column("webhook_secrets", postgresql.JSONB(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("partner_id"),
        sa.UniqueConstraint("client_id"),
    )
    op.create_table(
        "webhook_deliveries",
        sa.Column("event_id", sa.String(length=64), nullable=False),
        sa.Column("partner_id", sa.String(length=64), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("event", postgresql.JSONB(), nullable=False),
        sa.Column("trace", postgresql.JSONB(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("history", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("event_id"),
    )
    op.create_index(
        "ix_deliveries_due",
        "webhook_deliveries",
        ["next_attempt_at"],
        unique=False,
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index("ix_deliveries_partner_created", "webhook_deliveries", ["partner_id", "created_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_deliveries_partner_created", table_name="webhook_deliveries")
    op.drop_index("ix_deliveries_due", table_name="webhook_deliveries", postgresql_where=sa.text("status = 'pending'"))
    op.drop_table("webhook_deliveries")
    op.drop_table("partners")
    op.drop_index("ix_commands_pending_created", table_name="commands", postgresql_where=sa.text("status = 'pending'"))
    op.drop_index("ix_commands_partner", table_name="commands")
    op.drop_table("commands")
