"""Idempotent consumption: the mark_processed guard and handler dedup.

A redelivered event must not be processed twice — no duplicate downstream event,
no re-run of the LLM. Uses in-memory SQLite + fakes (no Kafka, no network).
"""

from __future__ import annotations

from sqlalchemy import func, select

from docstream.common.events import (
    DocumentExtracted,
    DocumentIngested,
    make_event,
)
from docstream.db.idempotency import mark_processed
from docstream.db.models import Job, JobStatus, OutboxEvent, ProcessedEvent
from docstream.enrichment.embedding import FakeEmbedder
from docstream.enrichment.llm import EnrichmentResult
from docstream.enrichment.worker import handle_extracted
from docstream.extraction.worker import handle_ingested
from docstream.gateway.service import create_ingestion_job
from docstream.storage.local import LocalStorage


class FakeQdrant:
    """Records upserts instead of talking to a real Qdrant cluster."""

    def __init__(self) -> None:
        self.points: dict[str, dict] = {}

    async def upsert(self, collection_name: str, points) -> None:
        store = self.points.setdefault(collection_name, {})
        for p in points:
            store[p.id] = (p.vector, p.payload)


class CountingLLM:
    """FakeLLM that records how many times enrich() was called."""

    def __init__(self) -> None:
        self.calls = 0

    async def enrich(self, text: str) -> EnrichmentResult:
        self.calls += 1
        return EnrichmentResult(classification="doc", summary="s", fields={})


# --------------------------------------------------------------------------- #
# The guard itself
# --------------------------------------------------------------------------- #
async def test_mark_processed_claims_once(sessionmaker):
    async with sessionmaker() as session:
        async with session.begin():
            first = await mark_processed(session, "evt-1", "group-a")
            second = await mark_processed(session, "evt-1", "group-a")
        assert first is True
        assert second is False


async def test_mark_processed_is_per_group(sessionmaker):
    """The same event can be processed once by each consumer group (fan-out)."""
    async with sessionmaker() as session:
        async with session.begin():
            a = await mark_processed(session, "evt-1", "group-a")
            b = await mark_processed(session, "evt-1", "group-b")
        assert a is True and b is True

    async with sessionmaker() as session:
        count = (
            await session.execute(
                select(func.count()).select_from(ProcessedEvent)
            )
        ).scalar_one()
        assert count == 2


# --------------------------------------------------------------------------- #
# Enrichment handler: duplicate extracted event is a no-op
# --------------------------------------------------------------------------- #
async def test_enrichment_skips_duplicate_event(sessionmaker, tmp_path):
    storage = LocalStorage(tmp_path / "store")
    qdrant = FakeQdrant()
    llm = CountingLLM()
    text = "Some document content here. " * 40

    # Seed a job + its extracted text.
    async with sessionmaker() as session:
        async with session.begin():
            job = await create_ingestion_job(
                session,
                filename="doc.txt",
                content_type="text/plain",
                size_bytes=len(text),
                storage_uri="",
            )
        document_id, job_id = job.document_id, job.id
    text_uri = storage.save_sync(document_id, "doc.txt.extracted.txt", text.encode())

    envelope = make_event(
        DocumentExtracted(
            job_id=job_id,
            document_id=document_id,
            text_uri=text_uri,
            text_length=len(text),
        ),
        source="test",
    )

    # Deliver the SAME event twice.
    for _ in range(2):
        async with sessionmaker() as session:
            async with session.begin():
                await handle_extracted(
                    session,
                    storage,
                    qdrant,
                    FakeEmbedder(dim=8),
                    llm,
                    envelope,
                    collection="documents",
                    chunk_size=80,
                    chunk_overlap=16,
                )

    # LLM ran exactly once; exactly one documents.enriched event was emitted.
    assert llm.calls == 1
    async with sessionmaker() as session:
        enriched = (
            await session.execute(
                select(func.count())
                .select_from(OutboxEvent)
                .where(OutboxEvent.topic == "documents.enriched")
            )
        ).scalar_one()
        assert enriched == 1

        job = (await session.execute(select(Job).where(Job.id == job_id))).scalar_one()
        assert job.status == JobStatus.COMPLETED


# --------------------------------------------------------------------------- #
# Extraction handler: duplicate ingested event is a no-op
# --------------------------------------------------------------------------- #
async def test_extraction_skips_duplicate_event(sessionmaker, tmp_path):
    storage = LocalStorage(tmp_path / "store")

    async with sessionmaker() as session:
        async with session.begin():
            job = await create_ingestion_job(
                session,
                filename="lease.txt",
                content_type="text/plain",
                size_bytes=11,
                storage_uri="",
            )
        document_id, job_id = job.document_id, job.id
    storage_uri = storage.save_sync(document_id, "lease.txt", b"lease terms")
    async with sessionmaker() as session:
        async with session.begin():
            j = (await session.execute(select(Job).where(Job.id == job_id))).scalar_one()
            j.storage_uri = storage_uri

    envelope = make_event(
        DocumentIngested(
            job_id=job_id,
            document_id=document_id,
            filename="lease.txt",
            content_type="text/plain",
            size_bytes=11,
            storage_uri=storage_uri,
        ),
        source="test",
    )

    for _ in range(2):
        async with sessionmaker() as session:
            async with session.begin():
                await handle_ingested(session, storage, envelope)

    async with sessionmaker() as session:
        extracted = (
            await session.execute(
                select(func.count())
                .select_from(OutboxEvent)
                .where(OutboxEvent.topic == "documents.extracted")
            )
        ).scalar_one()
        assert extracted == 1
