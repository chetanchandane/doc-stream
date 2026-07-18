"""job chunk_count: number of chunks embedded during enrichment

Adds a nullable column so the /jobs API can report how many chunks were indexed
without reading the Kafka event. Separate from 0002 because 0002 is already
applied in dev environments.

Revision ID: 0003_job_chunk_count
Revises: 0002_enrichment_results
Create Date: 2026-07-17
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_job_chunk_count"
down_revision: str | None = "0002_enrichment_results"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("chunk_count", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("jobs", "chunk_count")
