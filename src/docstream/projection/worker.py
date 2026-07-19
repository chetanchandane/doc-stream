"""Query projector — the read side of CQRS.

Consumes ``documents.enriched`` and upserts a denormalized ``document_view``
row. This is the only writer of the read model; the command side (gateway +
extraction/enrichment workers) never touches it.

Why a projector at all: the write model (``jobs``) is shaped for pipeline state
transitions, while the read API wants a flat, query-optimized row. Separating
them lets each evolve and scale on its own, at the cost of eventual consistency
— a document is queryable a moment after it's enriched, not the instant the job
row flips to completed.

The projection is deliberately a *pure function of the event*: everything it
needs (including ``filename``) travels on ``DocumentEnriched``, so the read side
never reads the write side.

Run it standalone:

    python -m docstream.projection.worker
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docstream.common.config import get_settings
from docstream.common.events import DocumentEnriched, EventEnvelope
from docstream.common.retry import process_with_retry
from docstream.common.topics import DOCUMENTS_ENRICHED, GROUP_QUERY_PROJECTOR
from docstream.db.base import get_sessionmaker
from docstream.db.idempotency import mark_processed
from docstream.db.models import DocumentView

log = logging.getLogger("docstream.projection")


async def handle_enriched(session: AsyncSession, envelope: EventEnvelope) -> None:
    """Apply one enriched event to the read model. Caller owns the transaction.

    Upsert semantics: re-applying the same document (a corrected re-run, say)
    overwrites the row rather than duplicating it.
    """
    assert isinstance(envelope.payload, DocumentEnriched)
    payload = envelope.payload

    # Same idempotency guard the pipeline workers use, scoped to this group.
    if not await mark_processed(session, envelope.event_id, GROUP_QUERY_PROJECTOR):
        log.info("duplicate event %s; already projected, skipping", envelope.event_id)
        return

    view = await session.get(DocumentView, payload.document_id)
    if view is None:
        view = DocumentView(document_id=payload.document_id, job_id=payload.job_id)
        session.add(view)

    view.job_id = payload.job_id
    view.filename = payload.filename
    view.classification = payload.classification
    view.summary = payload.summary
    view.chunk_count = payload.chunk_count

    log.info(
        "projected document_id=%s classification=%s chunks=%d",
        payload.document_id,
        payload.classification,
        payload.chunk_count,
    )


async def _count_views(session: AsyncSession) -> int:
    """Small helper used by tests/diagnostics."""
    result = await session.execute(select(DocumentView))
    return len(result.scalars().all())


async def run_worker(stop_event: asyncio.Event | None = None) -> None:
    """Consume documents.enriched until cancelled."""
    from docstream.common.health import start_health_server
    from docstream.common.messaging import KafkaConsumer, KafkaProducer

    settings = get_settings()
    sm = get_sessionmaker()

    producer = KafkaProducer(settings.kafka)
    await producer.start()

    # Gives Kubernetes something to probe: a hung worker fails liveness
    # instead of silently consuming nothing.
    health = await start_health_server(settings.worker.health_port)

    consumer = KafkaConsumer(
        settings.kafka,
        topics=(DOCUMENTS_ENRICHED,),
        group_id=GROUP_QUERY_PROJECTOR,
    )
    await consumer.start()
    log.info("projector started; consuming %s", DOCUMENTS_ENRICHED)
    try:
        async for record in consumer:
            if stop_event is not None and stop_event.is_set():
                break
            envelope = EventEnvelope.from_bytes(record.value)

            async def _work(envelope: EventEnvelope = envelope) -> None:
                async with sm() as session:
                    async with session.begin():
                        await handle_enriched(session, envelope)

            # Same retry/DLQ policy as the pipeline workers. No on_dlq callback:
            # the read model has no job status to fail, the event just parks.
            await process_with_retry(
                _work,
                envelope=envelope,
                producer=producer,
                source_topic=DOCUMENTS_ENRICHED,
                max_attempts=settings.consumer.max_attempts,
                backoff_seconds=settings.consumer.backoff_seconds,
            )
            await consumer.commit()
    finally:
        await consumer.stop()
        await producer.stop()
        health.close()


def main() -> None:
    logging.basicConfig(
        level=get_settings().log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
