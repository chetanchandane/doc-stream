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
from datetime import UTC, datetime

from sqlalchemy import (
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy import (
    Enum as SAEnum,
)
from sqlalchemy.orm import Mapped, mapped_column

from docstream.db.base import Base


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _uuid() -> str:
    return str(uuid.uuid4())


class JobStatus(str, enum.Enum):
    PENDING = "pending"  # ingested, waiting for extraction
    EXTRACTING = "extracting"  # extraction worker is pulling text
    EXTRACTED = "extracted"  # text pulled, waiting for enrichment
    ENRICHING = "enriching"  # enrichment worker is running
    COMPLETED = "completed"  # enriched and indexed
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

    # Enrichment results (populated by the enrichment worker in Phase 2).
    classification: Mapped[str | None] = mapped_column(String(255), nullable=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    chunk_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

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


class DocumentView(Base):
    """CQRS read model: one row per fully enriched document.

    Built asynchronously by the projector from ``documents.enriched`` events —
    never written by the command side. The query service reads ONLY this table
    (plus Qdrant) so read traffic never touches the ``jobs`` write model and the
    two sides can evolve and scale independently.

    Denormalized on purpose: everything the read API needs lives here, so a
    query is a single-row lookup with no joins back to the write side.
    """

    __tablename__ = "document_view"

    document_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    job_id: Mapped[str] = mapped_column(String(36), index=True)

    filename: Mapped[str | None] = mapped_column(String(512), nullable=True)
    classification: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    chunk_count: Mapped[int] = mapped_column(Integer, default=0)

    # When the projector last applied an event for this document.
    indexed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow
    )


class ProcessedEvent(Base):
    """Dedup ledger for idempotent consumption.

    Each consumer group records the events it has fully processed. A worker
    inserts ``(event_id, consumer_group)`` in the SAME transaction as its work,
    so a redelivered event hits the unique constraint and is skipped instead of
    being processed twice. The row commits (or rolls back) atomically with the
    handler's DB writes and outbox emit.
    """

    __tablename__ = "processed_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    event_id: Mapped[str] = mapped_column(String(36), index=True)
    consumer_group: Mapped[str] = mapped_column(String(255))
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow
    )

    __table_args__ = (
        # One event may be processed once per consumer group (fan-out safe).
        UniqueConstraint("event_id", "consumer_group", name="uq_processed_event_group"),
    )
