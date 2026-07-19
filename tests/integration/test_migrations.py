"""Migrations against a REAL Postgres.

The unit suite builds its schema with ``Base.metadata.create_all`` on SQLite, so
it never executes a single alembic revision. That means a broken revision chain,
a bad down_revision, or Postgres-specific DDL would sail through the unit tests
and fail on deploy. This runs the actual migrations on actual Postgres.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

EXPECTED_TABLES = {"jobs", "outbox", "processed_events", "document_view"}


async def test_upgrade_head_creates_the_full_schema(postgres_dsn, alembic_upgrade):
    await alembic_upgrade(postgres_dsn, "head")

    engine = create_async_engine(postgres_dsn)
    try:
        async with engine.connect() as conn:
            rows = await conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='public'"
                )
            )
            tables = {r[0] for r in rows}
            assert EXPECTED_TABLES <= tables, f"missing: {EXPECTED_TABLES - tables}"

            # Columns added by later revisions actually landed.
            cols = await conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name='jobs'"
                )
            )
            job_cols = {r[0] for r in cols}
            assert {"classification", "summary", "chunk_count"} <= job_cols

            # The idempotency guard depends on this unique constraint existing.
            constraints = await conn.execute(
                text(
                    "SELECT constraint_name FROM information_schema.table_constraints "
                    "WHERE table_name='processed_events' AND constraint_type='UNIQUE'"
                )
            )
            names = {r[0] for r in constraints}
            assert "uq_processed_event_group" in names
    finally:
        await engine.dispose()


async def test_upgrade_is_rerunnable(postgres_dsn, alembic_upgrade):
    """Running upgrade head twice is a no-op, not an error."""
    await alembic_upgrade(postgres_dsn, "head")
    await alembic_upgrade(postgres_dsn, "head")
