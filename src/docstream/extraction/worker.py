"""Extraction worker.

Consumes ``documents.ingested``, pulls text from the stored bytes, updates the
job, and emits ``documents.extracted`` — the emit goes through the **same
outbox** the gateway uses, written in the same transaction as the job update.
So every service in the pipeline produces events the same reliable way, and the
existing relay drains them to Kafka.

Run it standalone:

    python -m docstream.extraction.worker
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docstream.common.config import get_settings
from docstream.common.events import (
    DocumentExtracted,
    DocumentIngested,
    EventEnvelope,
    make_event,
)
from docstream.common.topics import DOCUMENTS_INGESTED, GROUP_EXTRACTION
from docstream.db.base import get_sessionmaker
from docstream.db.models import Job, JobStatus
from docstream.db.outbox import enqueue_event
from docstream.extraction.text import extract_text
from docstream.storage import LocalStorage, get_storage

log = logging.getLogger("docstream.extraction")

SOURCE = "extraction-worker"


async def handle_ingested(
    session: AsyncSession,
    storage: LocalStorage,
    envelope: EventEnvelope,
) -> None:
    """Process one ingested event. The caller owns the transaction/commit."""
    assert isinstance(envelope.payload, DocumentIngested)
    payload = envelope.payload

    job = await _load_job(session, payload.document_id)
    if job is None:
        # Event for an unknown job: nothing to do. (Job store is the source of
        # truth; a missing job means it was never written or was purged.)
        log.warning("no job for document_id=%s; skipping", payload.document_id)
        return

    job.status = JobStatus.EXTRACTING

    data = storage.read(payload.storage_uri)
    result = extract_text(payload.filename, payload.content_type, data)
    text_uri = storage.save(
        payload.document_id, f"{payload.filename}.extracted.txt", result.text.encode()
    )

    extracted = make_event(
        DocumentExtracted(
            job_id=job.id,
            document_id=payload.document_id,
            text_uri=text_uri,
            text_length=len(result.text),
            page_count=result.page_count,
            extraction_method=result.method,
        ),
        source=SOURCE,
        correlation_id=envelope.correlation_id,  # preserve the trace
    )
    enqueue_event(session, extracted)

    job.status = JobStatus.EXTRACTED
    log.info(
        "extracted document_id=%s chars=%d method=%s",
        payload.document_id,
        len(result.text),
        result.method,
    )


async def _load_job(session: AsyncSession, document_id: str) -> Job | None:
    result = await session.execute(select(Job).where(Job.document_id == document_id))
    return result.scalar_one_or_none()


async def _mark_failed(document_id: str, error: str) -> None:
    """Best-effort FAILED update in its own transaction after a handler error.

    Week 1 has no retry/DLQ yet (that's Week 2), so we record the failure and
    move on rather than looping on a poison message.
    """
    sm = get_sessionmaker()
    try:
        async with sm() as session:
            async with session.begin():
                job = await _load_job(session, document_id)
                if job is not None:
                    job.status = JobStatus.FAILED
                    job.error = error[:2000]
    except Exception:  # noqa: BLE001
        log.exception("failed to mark job failed for document_id=%s", document_id)


async def run_worker(stop_event: asyncio.Event | None = None) -> None:
    """Consume documents.ingested until cancelled."""
    from docstream.common.messaging import KafkaConsumer

    settings = get_settings()
    storage = get_storage()
    sm = get_sessionmaker()

    consumer = KafkaConsumer(
        settings.kafka,
        topics=(DOCUMENTS_INGESTED,),
        group_id=GROUP_EXTRACTION,
    )
    await consumer.start()
    log.info("extraction worker started; consuming %s", DOCUMENTS_INGESTED)
    try:
        async for record in consumer:
            if stop_event is not None and stop_event.is_set():
                break
            envelope = EventEnvelope.from_bytes(record.value)
            try:
                async with sm() as session:
                    async with session.begin():
                        await handle_ingested(session, storage, envelope)
            except Exception as exc:  # noqa: BLE001
                log.exception("extraction failed for event %s", envelope.event_id)
                await _mark_failed(envelope.document_id, repr(exc))
            # Commit the offset either way: success advances, failure is recorded
            # as FAILED (proper retry + DLQ arrives in Week 2).
            await consumer.commit()
    finally:
        await consumer.stop()


def main() -> None:
    logging.basicConfig(
        level=get_settings().log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
