"""Outbox helpers shared by the write path (enqueue) and the relay (drain)."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docstream.common.events import EventEnvelope, EventType
from docstream.common.topics import (
    DOCUMENTS_ENRICHED,
    DOCUMENTS_EXTRACTED,
    DOCUMENTS_INGESTED,
)
from docstream.db.models import OutboxEvent

_TOPIC_BY_EVENT: dict[EventType, str] = {
    EventType.DOCUMENT_INGESTED: DOCUMENTS_INGESTED,
    EventType.DOCUMENT_EXTRACTED: DOCUMENTS_EXTRACTED,
    EventType.DOCUMENT_ENRICHED: DOCUMENTS_ENRICHED,
}


def topic_for(envelope: EventEnvelope) -> str:
    """Map an event to the topic it publishes on."""
    return _TOPIC_BY_EVENT[envelope.event_type]


def enqueue_event(session: AsyncSession, envelope: EventEnvelope) -> OutboxEvent:
    """Stage an event in the outbox table.

    Must be called inside the same transaction as the state change that produced
    the event. Does not commit — the caller owns the transaction boundary.
    """
    row = OutboxEvent(
        event_id=envelope.event_id,
        aggregate_id=envelope.document_id,
        topic=topic_for(envelope),
        key=envelope.document_id,
        payload=envelope.to_bytes().decode("utf-8"),
    )
    session.add(row)
    return row


async def fetch_unpublished(session: AsyncSession, limit: int) -> Sequence[OutboxEvent]:
    """Return the oldest unpublished rows, locking them on Postgres so multiple
    relay replicas don't publish the same event twice."""
    stmt = (
        select(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None))
        .order_by(OutboxEvent.created_at)
        .limit(limit)
    )
    # SELECT ... FOR UPDATE SKIP LOCKED is a Postgres feature; SQLite ignores it.
    if session.bind is not None and session.bind.dialect.name == "postgresql":
        stmt = stmt.with_for_update(skip_locked=True)
    result = await session.execute(stmt)
    return result.scalars().all()


def mark_published(row: OutboxEvent) -> None:
    row.published_at = datetime.now(timezone.utc)
