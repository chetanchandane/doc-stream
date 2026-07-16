"""initial: jobs + outbox

Revision ID: 0001_initial
Revises:
Create Date: 2026-07-16
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_JOB_STATUS = sa.Enum(
    "pending",
    "extracting",
    "enriching",
    "completed",
    "failed",
    name="job_status",
    native_enum=False,
    length=20,
)


def upgrade() -> None:
    op.create_table(
        "jobs",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("document_id", sa.String(length=36), nullable=False),
        sa.Column("filename", sa.String(length=512), nullable=False),
        sa.Column("content_type", sa.String(length=255), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("storage_uri", sa.String(length=1024), nullable=False),
        sa.Column("status", _JOB_STATUS, nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_jobs_document_id", "jobs", ["document_id"], unique=True)
    op.create_index("ix_jobs_status", "jobs", ["status"], unique=False)

    op.create_table(
        "outbox",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("event_id", sa.String(length=36), nullable=False),
        sa.Column("aggregate_id", sa.String(length=36), nullable=False),
        sa.Column("topic", sa.String(length=255), nullable=False),
        sa.Column("key", sa.String(length=255), nullable=True),
        sa.Column("payload", sa.Text(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_outbox_event_id", "outbox", ["event_id"], unique=True)
    op.create_index("ix_outbox_aggregate_id", "outbox", ["aggregate_id"], unique=False)
    op.create_index(
        "ix_outbox_unpublished", "outbox", ["published_at", "created_at"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_unpublished", table_name="outbox")
    op.drop_index("ix_outbox_aggregate_id", table_name="outbox")
    op.drop_index("ix_outbox_event_id", table_name="outbox")
    op.drop_table("outbox")
    op.drop_index("ix_jobs_status", table_name="jobs")
    op.drop_index("ix_jobs_document_id", table_name="jobs")
    op.drop_table("jobs")
