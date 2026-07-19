"""Enrichment worker.

Consumes ``documents.extracted``, embeds the extracted text into Qdrant, runs
LLM classification + summarization, and emits ``documents.enriched`` — the emit
goes through the **same outbox** the gateway and extraction worker use, written
in the same transaction as the job update. The existing relay drains it to Kafka.

This is the mirror image of ``extraction/worker.py``: a pure ``handle_extracted``
that owns no I/O lifecycle (the caller opens the transaction and commits), plus a
``run_worker`` loop that builds the real clients and wires Kafka.

Run it standalone:

    python -m docstream.enrichment.worker
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docstream.common.config import get_settings
from docstream.common.events import (
    DocumentEnriched,
    DocumentExtracted,
    EventEnvelope,
    make_event,
)
from docstream.common.retry import process_with_retry
from docstream.common.topics import DOCUMENTS_EXTRACTED, GROUP_ENRICHMENT
from docstream.db.base import get_sessionmaker
from docstream.db.idempotency import mark_processed
from docstream.db.models import Job, JobStatus
from docstream.db.outbox import enqueue_event
from docstream.enrichment.chunking import chunk_text
from docstream.enrichment.embedding import Embedder
from docstream.enrichment.llm import LLM
from docstream.enrichment.qdrant_store import upsert_chunks
from docstream.storage import Storage, get_storage

log = logging.getLogger("docstream.enrichment")

SOURCE = "enrichment-worker"


async def handle_extracted(
    session: AsyncSession,
    storage: Storage,
    qdrant,
    embedder: Embedder,
    llm: LLM,
    envelope: EventEnvelope,
    *,
    collection: str,
    chunk_size: int = 512,
    chunk_overlap: int = 64,
) -> None:
    """Process one extracted event. The caller owns the transaction/commit.

    Steps: load job -> ENRICHING -> chunk + embed -> upsert vectors ->
    LLM enrich -> stage ``documents.enriched`` in the outbox -> COMPLETED.
    Deterministic Qdrant point ids make the upsert safe to replay (full
    idempotency guard arrives in Phase 3).
    """
    assert isinstance(envelope.payload, DocumentExtracted)
    payload = envelope.payload

    # Idempotency guard: skip if this consumer group already processed the event.
    # Deterministic point ids keep the Qdrant upsert safe too, but this stops a
    # redelivered event from re-running the LLM and re-emitting documents.enriched.
    if not await mark_processed(session, envelope.event_id, GROUP_ENRICHMENT):
        log.info("duplicate event %s; already processed, skipping", envelope.event_id)
        return

    job = await _load_job(session, payload.document_id)
    if job is None:
        # Job store is the source of truth; a missing job means it was never
        # written or was purged. Nothing to enrich.
        log.warning("no job for document_id=%s; skipping", payload.document_id)
        return

    job.status = JobStatus.ENRICHING

    text = (await storage.read(payload.text_uri)).decode("utf-8")
    chunks = chunk_text(text, chunk_size=chunk_size, chunk_overlap=chunk_overlap)

    vectors = await embedder.embed(chunks)
    vector_ids = await upsert_chunks(
        qdrant, collection, payload.document_id, chunks, vectors
    )

    result = await llm.enrich(text)

    enriched = make_event(
        DocumentEnriched(
            job_id=job.id,
            document_id=payload.document_id,
            filename=job.filename,  # lets the read-side projector avoid the write model
            classification=result.classification,
            summary=result.summary,
            chunk_count=len(chunks),
            vector_ids=vector_ids,
        ),
        source=SOURCE,
        correlation_id=envelope.correlation_id,  # preserve the trace
    )
    enqueue_event(session, enriched)

    job.classification = result.classification
    job.summary = result.summary
    job.chunk_count = len(chunks)
    job.status = JobStatus.COMPLETED
    log.info(
        "enriched document_id=%s chunks=%d classification=%s",
        payload.document_id,
        len(chunks),
        result.classification,
    )


async def _load_job(session: AsyncSession, document_id: str) -> Job | None:
    result = await session.execute(select(Job).where(Job.document_id == document_id))
    return result.scalar_one_or_none()


async def _mark_failed(document_id: str, error: str) -> None:
    """Best-effort FAILED update in its own transaction after a handler error.

    Retry + DLQ arrive in Phase 4; for now we record the failure and commit the
    offset rather than looping on a poison message.
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


def _build_dependencies(settings):
    """Construct the real embedder, LLM, and Qdrant client from settings.

    Kept separate from ``run_worker`` so it is easy to see (and swap) the only
    place provider SDKs are instantiated.
    """
    from anthropic import AsyncAnthropic
    from openai import AsyncOpenAI
    from qdrant_client import AsyncQdrantClient

    from docstream.enrichment.embedding import OpenAIEmbedder
    from docstream.enrichment.llm import AnthropicLLM

    openai_client = AsyncOpenAI(
        api_key=settings.embedding.api_key,
        base_url=settings.embedding.base_url or None,
    )
    embedder = OpenAIEmbedder(
        openai_client, model=settings.embedding.model, dim=settings.embedding.dim
    )

    anthropic_client = AsyncAnthropic(
        api_key=settings.llm.api_key,
        base_url=settings.llm.base_url or None,
    )
    llm = AnthropicLLM(
        anthropic_client, model=settings.llm.model, max_tokens=settings.llm.max_tokens
    )

    qdrant = AsyncQdrantClient(
        url=settings.qdrant.url, api_key=settings.qdrant.api_key
    )
    return embedder, llm, qdrant


async def run_worker(stop_event: asyncio.Event | None = None) -> None:
    """Consume documents.extracted until cancelled."""
    from docstream.common.health import start_health_server
    from docstream.common.messaging import KafkaConsumer, KafkaProducer
    from docstream.enrichment.qdrant_store import ensure_collection

    settings = get_settings()
    storage = get_storage()
    sm = get_sessionmaker()

    embedder, llm, qdrant = _build_dependencies(settings)
    # Idempotent: safe to call on every start.
    await ensure_collection(qdrant, settings.qdrant.collection, settings.qdrant.vector_size)

    producer = KafkaProducer(settings.kafka)
    await producer.start()

    # Gives Kubernetes something to probe: a hung worker fails liveness
    # instead of silently consuming nothing.
    health = await start_health_server(settings.worker.health_port)

    consumer = KafkaConsumer(
        settings.kafka,
        topics=(DOCUMENTS_EXTRACTED,),
        group_id=GROUP_ENRICHMENT,
    )
    await consumer.start()
    log.info("enrichment worker started; consuming %s", DOCUMENTS_EXTRACTED)
    try:
        async for record in consumer:
            if stop_event is not None and stop_event.is_set():
                break
            envelope = EventEnvelope.from_bytes(record.value)

            async def _work(envelope: EventEnvelope = envelope) -> None:
                async with sm() as session:
                    async with session.begin():
                        await handle_extracted(
                            session,
                            storage,
                            qdrant,
                            embedder,
                            llm,
                            envelope,
                            collection=settings.qdrant.collection,
                            chunk_size=settings.embedding.chunk_size,
                            chunk_overlap=settings.embedding.chunk_overlap,
                        )

            async def _on_dlq(exc: BaseException, envelope: EventEnvelope = envelope) -> None:
                await _mark_failed(envelope.document_id, repr(exc))

            # Retry on the same topic; dead-letter + mark failed once exhausted.
            await process_with_retry(
                _work,
                envelope=envelope,
                producer=producer,
                source_topic=DOCUMENTS_EXTRACTED,
                max_attempts=settings.consumer.max_attempts,
                backoff_seconds=settings.consumer.backoff_seconds,
                on_dlq=_on_dlq,
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
