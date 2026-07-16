"""ORM models: the job state store and the transactional outbox.

The two tables are the heart of the transactional-outbox pattern. A request
writes a ``jobs`` row *and* an ``outbox`` row in the **same** transaction, so a
job can never exist without its "ingested" event queued for publication (and
vice versa). A separate relay drains the outbox to Kafka.

Types are kept portable (VARCHAR enums, timezone-aware DateTime, Text payload)
so the same models run on Postgres in production and SQLite in tests.
"""

from __future__ import annotations

import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    DateTime,
    Enum as SAEnum,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column

from docstream.db.base import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _uuid() -> str:
    return str(uuid.uuid4())


class JobStatus(str, enum.Enum):
    PENDING = "pending"
    EXTRACTING = "extracting"
    ENRICHING = "enriching"
    COMPLETED = "completed"
    FAILED = "failed"


class Job(Base):
    """One row per ingested document; tracks its progress through the pipeline."""

    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    document_id: Mapped[str] = mapped_column(String(36), unique=True, index=True)

    filename: Mapped[str] = mapped_column(String(512))
    content_type: Mapped[str] = mapped_column(String(255))
    size_bytes: Mapped[int] = mapped_column(Integer)
    storage_uri: Mapped[str] = mapped_column(String(1024))

    status: Mapped[JobStatus] = mapped_column(
        SAEnum(JobStatus, native_enum=False, length=20),
        default=JobStatus.PENDING,
        index=True,
    )
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow
    )


class OutboxEvent(Base):
    """Durable queue of events waiting to be published to Kafka.

    ``published_at IS NULL`` means "not yet on the bus". The relay flips it once
    the send is acknowledged, giving at-least-once delivery (idempotent
    consumers deduplicate on ``event_id``).
    """

    __tablename__ = "outbox"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    # The envelope's event_id — the dedup key consumers use.
    event_id: Mapped[str] = mapped_column(String(36), unique=True, index=True)
    # The aggregate this event belongs to (document_id); also the Kafka key.
    aggregate_id: Mapped[str] = mapped_column(String(36), index=True)

    topic: Mapped[str] = mapped_column(String(255))
    key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    payload: Mapped[str] = mapped_column(Text)  # serialized EventEnvelope JSON

    attempts: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        # The relay's hot path: find the oldest unpublished rows fast.
        Index("ix_outbox_unpublished", "published_at", "created_at"),
    )
