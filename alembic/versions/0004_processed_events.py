"""processed_events: idempotent-consumption ledger

One row per (event_id, consumer_group) a worker has fully processed. The unique
constraint is the dedup guard: a redelivered event fails to insert and is
skipped.

Revision ID: 0004_processed_events
Revises: 0003_job_chunk_count
Create Date: 2026-07-17
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_processed_events"
down_revision: str | None = "0003_job_chunk_count"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "processed_events",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("event_id", sa.String(length=36), nullable=False),
        sa.Column("consumer_group", sa.String(length=255), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "event_id", "consumer_group", name="uq_processed_event_group"
        ),
    )
    op.create_index(
        "ix_processed_events_event_id", "processed_events", ["event_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_processed_events_event_id", table_name="processed_events")
    op.drop_table("processed_events")
