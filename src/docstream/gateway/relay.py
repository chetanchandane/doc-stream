"""Transactional-outbox relay.

Polls the ``outbox`` table for unpublished rows, publishes each to Kafka, and
marks it published in the same transaction that locked it. This is the piece
that turns "durably recorded intent" into "on the bus", giving at-least-once
delivery. Idempotent consumers (Week 2) dedupe any redelivery on ``event_id``.

Run it standalone:

    python -m docstream.gateway.relay

or let the gateway start it in-process (see ``DOCSTREAM_RELAY__RUN_IN_PROCESS``).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from docstream.common import metrics
from docstream.common.config import get_settings
from docstream.db.base import get_sessionmaker
from docstream.db.outbox import fetch_unpublished, mark_published

log = logging.getLogger("docstream.relay")


class Publisher(Protocol):
    async def publish(self, topic: str, value: bytes, key: bytes | None = None) -> None: ...


async def drain_once(
    session: AsyncSession, producer: Publisher, batch_size: int
) -> int:
    """Publish one batch of pending events. Returns how many were published.

    The publish happens before the row is marked and the transaction commits, so
    a crash mid-batch re-delivers rather than dropping. That is the deliberate
    at-least-once trade-off.
    """
    rows = await fetch_unpublished(session, batch_size)
    # Backlog depth. Only exact when the batch isn't full; a reading pinned at
    # batch_size means "at least this many", which is itself the signal that the
    # relay is falling behind.
    metrics.outbox_pending.set(len(rows))
    if not rows:
        return 0
    for row in rows:
        key = row.key.encode("utf-8") if row.key else None
        await producer.publish(row.topic, value=row.payload.encode("utf-8"), key=key)
        mark_published(row)
    await session.commit()
    log.info("relay published %d event(s)", len(rows))
    return len(rows)


async def run_relay(
    stop_event: asyncio.Event | None = None,
    *,
    sessionmaker: async_sessionmaker[AsyncSession] | None = None,
    producer: Publisher | None = None,
) -> None:
    """Long-running relay loop. Owns a Kafka producer unless one is injected."""
    settings = get_settings()
    sm = sessionmaker or get_sessionmaker()

    owns_producer = producer is None
    if producer is None:
        from docstream.common.messaging import KafkaProducer

        producer = KafkaProducer(settings.kafka)
        await producer.start()  # type: ignore[union-attr]

    log.info("outbox relay started (batch=%d)", settings.relay.batch_size)
    try:
        while stop_event is None or not stop_event.is_set():
            try:
                async with sm() as session:
                    published = await drain_once(
                        session, producer, settings.relay.batch_size
                    )
            except Exception:  # noqa: BLE001 - keep the relay alive on transient errors
                log.exception("relay batch failed; retrying")
                published = 0
            # Back off only when idle so a backlog drains quickly.
            if published == 0:
                await asyncio.sleep(settings.relay.poll_interval_seconds)
    finally:
        if owns_producer:
            await producer.stop()  # type: ignore[union-attr]


def main() -> None:
    logging.basicConfig(
        level=get_settings().log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run_relay())


if __name__ == "__main__":
    main()
