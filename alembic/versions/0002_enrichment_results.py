"""enrichment results: classification + summary on jobs

Adds the two nullable columns the enrichment worker (Phase 2) writes when a
document reaches ``completed``. Nullable so existing rows and in-flight jobs are
unaffected.

Revision ID: 0002_enrichment_results
Revises: 0001_initial
Create Date: 2026-07-16
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_enrichment_results"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("classification", sa.String(length=255), nullable=True))
    op.add_column("jobs", sa.Column("summary", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("jobs", "summary")
    op.drop_column("jobs", "classification")
