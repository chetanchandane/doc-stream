"""Enrichment: the pure adapters and the handler's DB + outbox + vector effects.

Uses in-memory fakes (FakeEmbedder, FakeLLM, FakeQdrant) so no network, no Kafka,
no real Qdrant — mirrors tests/test_extraction.py.
"""

from __future__ import annotations

from sqlalchemy import select

from docstream.common.events import DocumentExtracted, make_event
from docstream.db.models import Job, JobStatus, OutboxEvent
from docstream.enrichment.chunking import chunk_text
from docstream.enrichment.embedding import FakeEmbedder
from docstream.enrichment.llm import FakeLLM
from docstream.enrichment.qdrant_store import point_id
from docstream.enrichment.worker import handle_extracted
from docstream.gateway.service import create_ingestion_job
from docstream.storage.local import LocalStorage


class FakeQdrant:
    """Records upserts instead of talking to a real Qdrant cluster."""

    def __init__(self) -> None:
        # collection -> {point_id: (vector, payload)}
        self.points: dict[str, dict] = {}

    async def upsert(self, collection_name: str, points) -> None:
        store = self.points.setdefault(collection_name, {})
        for p in points:
            store[p.id] = (p.vector, p.payload)


# --------------------------------------------------------------------------- #
# Pure chunker
# --------------------------------------------------------------------------- #
def test_chunk_text_basic():
    text = "Alpha beta. " * 200
    chunks = chunk_text(text, chunk_size=80, chunk_overlap=16)
    assert chunks and all(isinstance(c, str) and c.strip() for c in chunks)


def test_chunk_text_empty():
    assert chunk_text("") == []
    assert chunk_text("   \n  ") == []


# --------------------------------------------------------------------------- #
# Handler: DB + outbox + vector effects
# --------------------------------------------------------------------------- #
async def _seed_extracted_job(sessionmaker, storage, text: str):
    """Create a job in EXTRACTED-ready state with its text in storage.

    Returns (envelope, document_id, job_id).
    """
    async with sessionmaker() as session:
        async with session.begin():
            job = await create_ingestion_job(
                session,
                filename="report.txt",
                content_type="text/plain",
                size_bytes=len(text),
                storage_uri="",
            )
        document_id, job_id = job.document_id, job.id

    text_uri = storage.save_sync(document_id, "report.txt.extracted.txt", text.encode())

    envelope = make_event(
        DocumentExtracted(
            job_id=job_id,
            document_id=document_id,
            text_uri=text_uri,
            text_length=len(text),
            page_count=1,
            extraction_method="native",
        ),
        source="test",
    )
    return envelope, document_id, job_id


async def test_handle_extracted_enriches_and_emits(sessionmaker, tmp_path):
    storage = LocalStorage(tmp_path / "store")
    qdrant = FakeQdrant()
    text = "Quarterly revenue rose. " * 100

    envelope, document_id, job_id = await _seed_extracted_job(sessionmaker, storage, text)

    async with sessionmaker() as session:
        async with session.begin():
            await handle_extracted(
                session,
                storage,
                qdrant,
                FakeEmbedder(dim=8),
                FakeLLM(classification="report"),
                envelope,
                collection="documents",
                chunk_size=80,
                chunk_overlap=16,
            )

    # Job completed with results persisted.
    async with sessionmaker() as session:
        job = (await session.execute(select(Job).where(Job.id == job_id))).scalar_one()
        assert job.status == JobStatus.COMPLETED
        assert job.classification == "report"
        assert job.summary  # non-empty
        assert job.chunk_count and job.chunk_count > 0

    # Exactly one documents.enriched event staged in the outbox.
    async with sessionmaker() as session:
        rows = (
            await session.execute(
                select(OutboxEvent).where(OutboxEvent.topic == "documents.enriched")
            )
        ).scalars().all()
        assert len(rows) == 1

    # Vectors upserted with deterministic ids.
    stored = qdrant.points["documents"]
    assert len(stored) > 0
    assert point_id(document_id, 0) in stored


async def test_handle_extracted_is_idempotent_on_vectors(sessionmaker, tmp_path):
    """Re-running the handler overwrites the same points (deterministic ids),
    so the vector count does not grow. (Full dedup guard lands in Phase 3.)"""
    storage = LocalStorage(tmp_path / "store")
    qdrant = FakeQdrant()
    text = "Same content every time. " * 50

    envelope, document_id, _ = await _seed_extracted_job(sessionmaker, storage, text)

    for _ in range(2):
        async with sessionmaker() as session:
            async with session.begin():
                await handle_extracted(
                    session,
                    storage,
                    qdrant,
                    FakeEmbedder(dim=8),
                    FakeLLM(),
                    envelope,
                    collection="documents",
                    chunk_size=80,
                    chunk_overlap=16,
                )

    expected_chunks = len(chunk_text(text, chunk_size=80, chunk_overlap=16))
    assert len(qdrant.points["documents"]) == expected_chunks


async def test_handle_extracted_missing_job_is_noop(sessionmaker, tmp_path):
    storage = LocalStorage(tmp_path / "store")
    qdrant = FakeQdrant()
    text_uri = storage.save_sync("ghost", "t.txt", b"orphan text")

    envelope = make_event(
        DocumentExtracted(
            job_id="nope",
            document_id="ghost",
            text_uri=text_uri,
            text_length=11,
        ),
        source="test",
    )

    async with sessionmaker() as session:
        async with session.begin():
            await handle_extracted(
                session,
                storage,
                qdrant,
                FakeEmbedder(dim=8),
                FakeLLM(),
                envelope,
                collection="documents",
            )

    # Nothing enriched, nothing upserted.
    assert qdrant.points == {}
