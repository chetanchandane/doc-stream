"""document_view: CQRS read model

Denormalized projection of enriched documents, maintained by the projector from
documents.enriched events. The query service reads only this table (plus
Qdrant); the command side never writes it.

Revision ID: 0005_document_view
Revises: 0004_processed_events
Create Date: 2026-07-17
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005_document_view"
down_revision: str | None = "0004_processed_events"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "document_view",
        sa.Column("document_id", sa.String(length=36), primary_key=True),
        sa.Column("job_id", sa.String(length=36), nullable=False),
        sa.Column("filename", sa.String(length=512), nullable=True),
        sa.Column("classification", sa.String(length=255), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("chunk_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("indexed_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_document_view_job_id", "document_view", ["job_id"])
    op.create_index(
        "ix_document_view_classification", "document_view", ["classification"]
    )


def downgrade() -> None:
    op.drop_index("ix_document_view_classification", table_name="document_view")
    op.drop_index("ix_document_view_job_id", table_name="document_view")
    op.drop_table("document_view")
