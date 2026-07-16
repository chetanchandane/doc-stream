"""Async SQLAlchemy engine + session management.

The engine and sessionmaker are created lazily from settings and cached. Tests
override them via :func:`configure` so they can point at an in-memory database
without touching Postgres.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from docstream.common.config import get_settings


class Base(DeclarativeBase):
    """Declarative base for every ORM model."""


_engine: AsyncEngine | None = None
_sessionmaker: async_sessionmaker[AsyncSession] | None = None


def configure(engine: AsyncEngine) -> None:
    """Install a specific engine (used by tests)."""
    global _engine, _sessionmaker
    _engine = engine
    _sessionmaker = async_sessionmaker(engine, expire_on_commit=False)


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        _engine = create_async_engine(get_settings().postgres.dsn, pool_pre_ping=True)
    return _engine


def get_sessionmaker() -> async_sessionmaker[AsyncSession]:
    global _sessionmaker
    if _sessionmaker is None:
        _sessionmaker = async_sessionmaker(get_engine(), expire_on_commit=False)
    return _sessionmaker


async def get_session() -> AsyncSession:
    """FastAPI dependency yielding a session (see gateway for usage)."""
    async with get_sessionmaker()() as session:
        yield session
