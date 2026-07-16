"""Shared fixtures: an in-memory SQLite DB and a fake Kafka producer."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest_asyncio
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from docstream.db import base as db_base
from docstream.db.base import Base

# Import models so their tables register on the metadata.
from docstream.db import models  # noqa: F401


@pytest_asyncio.fixture
async def sessionmaker() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """A fresh in-memory SQLite database per test, wired into db.base."""
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,  # one shared connection => one in-memory db
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    db_base.configure(engine)  # so get_sessionmaker() / get_session() use this db
    sm = db_base.get_sessionmaker()
    try:
        yield sm
    finally:
        await engine.dispose()
        db_base._engine = None
        db_base._sessionmaker = None


class FakeProducer:
    """Records publishes instead of talking to Kafka."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, bytes | None]] = []

    async def publish(self, topic: str, value: bytes, key: bytes | None = None) -> None:
        self.published.append((topic, value, key))


@pytest_asyncio.fixture
async def producer() -> FakeProducer:
    return FakeProducer()
