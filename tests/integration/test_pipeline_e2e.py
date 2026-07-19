"""End-to-end pipeline against REAL Postgres and REAL Qdrant.

Drives the whole flow the way the brokers would, but in-process:

    create job (gateway) -> outbox
        -> handle_ingested  (extraction)  -> outbox
        -> handle_extracted (enrichment)  -> Qdrant + outbox
        -> handle_enriched  (projector)   -> document_view
        -> query service search           -> real vector hits

Embedding and LLM calls use fakes (no API keys, no cost, deterministic), but the
database, the outbox, the vector store, and every SQL statement are real. That's
the layer the unit suite can't reach: real transactions, real constraints, real
Qdrant round-trips.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select

from docstream.common.events import EventEnvelope
from docstream.db.models import DocumentView, Job, JobStatus, OutboxEvent
from docstream.enrichment.embedding import FakeEmbedder
from docstream.enrichment.llm import FakeLLM
from docstream.enrichment.qdrant_store import ensure_collection
from docstream.enrichment.worker import handle_extracted
from docstream.extraction.worker import handle_ingested
from docstream.gateway.service import create_ingestion_job
from docstream.projection.worker import handle_enriched
from docstream.query import service as query_service
from docstream.storage.local import LocalStorage

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

DIM = 8
SAMPLE = (
    "This lease agreement is between Acme Corp and the tenant. "
    "The security deposit is $2000, due on signing. "
    "The lease term is twelve months beginning in January. "
) * 3


@pytest.fixture
def sm(migrated_sessionmaker):
    """Alias for readability: a sessionmaker on real, migrated Postgres."""
    return migrated_sessionmaker


async def _drain_outbox(sm, topic: str) -> list[EventEnvelope]:
    """Stand in for the relay: read staged events of one topic off the outbox."""
    async with sm() as session:
        rows = (
            await session.execute(
                select(OutboxEvent).where(OutboxEvent.topic == topic)
            )
        ).scalars().all()
        return [EventEnvelope.from_bytes(r.payload.encode()) for r in rows]


async def test_full_pipeline_to_searchable_document(sm, qdrant_client, tmp_path):
    collection = f"e2e_{uuid.uuid4().hex[:8]}"
    await ensure_collection(qdrant_client, collection, DIM)
    storage = LocalStorage(tmp_path / "store")

    # --- 1. Ingest (gateway) -------------------------------------------------
    # Mirror the real gateway: persist the bytes FIRST (under a provisional id),
    # then create the job with the real URI. Order matters — the outbox event is
    # a snapshot taken when it's staged, so mutating the Job row afterwards
    # would NOT update the already-staged event.
    provisional_id = str(uuid.uuid4())
    storage_uri = storage.save_sync(provisional_id, "lease.txt", SAMPLE.encode())

    async with sm() as session:
        async with session.begin():
            job = await create_ingestion_job(
                session,
                filename="lease.txt",
                content_type="text/plain",
                size_bytes=len(SAMPLE),
                storage_uri=storage_uri,
            )
        document_id, job_id = job.document_id, job.id

    ingested = [
        e for e in await _drain_outbox(sm, "documents.ingested")
        if e.document_id == document_id
    ]
    assert len(ingested) == 1

    # --- 2. Extraction -------------------------------------------------------
    async with sm() as session:
        async with session.begin():
            await handle_ingested(session, storage, ingested[0])

    extracted = [
        e for e in await _drain_outbox(sm, "documents.extracted")
        if e.document_id == document_id
    ]
    assert len(extracted) == 1

    # --- 3. Enrichment (real Qdrant, fake embedder/LLM) ----------------------
    async with sm() as session:
        async with session.begin():
            await handle_extracted(
                session,
                storage,
                qdrant_client,
                FakeEmbedder(dim=DIM),
                FakeLLM(classification="lease"),
                extracted[0],
                collection=collection,
                chunk_size=120,
                chunk_overlap=24,
            )

    async with sm() as session:
        row = (await session.execute(select(Job).where(Job.id == job_id))).scalar_one()
        assert row.status == JobStatus.COMPLETED
        assert row.classification == "lease"
        assert row.chunk_count and row.chunk_count > 0

    # Vectors really landed in Qdrant.
    assert (await qdrant_client.count(collection_name=collection)).count == row.chunk_count

    enriched = [
        e for e in await _drain_outbox(sm, "documents.enriched")
        if e.document_id == document_id
    ]
    assert len(enriched) == 1
    assert enriched[0].payload.filename == "lease.txt"

    # --- 4. Projection (CQRS read model) ------------------------------------
    async with sm() as session:
        async with session.begin():
            await handle_enriched(session, enriched[0])

    async with sm() as session:
        view = await session.get(DocumentView, document_id)
        assert view is not None
        assert view.filename == "lease.txt"
        assert view.classification == "lease"

    # --- 5. Query the read side (real vector search) ------------------------
    async with sm() as session:
        hits = await query_service.search_chunks(
            session,
            embedder=FakeEmbedder(dim=DIM),
            qdrant=qdrant_client,
            collection=collection,
            question="security deposit",
            limit=3,
        )

    assert hits, "expected vector search to return hits"
    assert all(h["document_id"] == document_id for h in hits)
    # Filename joined from the read model -> citations are human-readable.
    assert all(h["filename"] == "lease.txt" for h in hits)


async def test_replayed_events_do_not_duplicate_anything(sm, qdrant_client, tmp_path):
    """Idempotency (Phase 3) holding up against real Postgres constraints."""
    collection = f"e2e_{uuid.uuid4().hex[:8]}"
    await ensure_collection(qdrant_client, collection, DIM)
    storage = LocalStorage(tmp_path / "store2")

    # Bytes first, then the job — see the note in the test above.
    provisional_id = str(uuid.uuid4())
    storage_uri = storage.save_sync(provisional_id, "dup.txt", SAMPLE.encode())

    async with sm() as session:
        async with session.begin():
            job = await create_ingestion_job(
                session,
                filename="dup.txt",
                content_type="text/plain",
                size_bytes=len(SAMPLE),
                storage_uri=storage_uri,
            )
        document_id, job_id = job.document_id, job.id

    ingested = next(
        e for e in await _drain_outbox(sm, "documents.ingested")
        if e.document_id == document_id
    )

    # Deliver the SAME ingested event twice.
    for _ in range(2):
        async with sm() as session:
            async with session.begin():
                await handle_ingested(session, storage, ingested)

    extracted = [
        e for e in await _drain_outbox(sm, "documents.extracted")
        if e.document_id == document_id
    ]
    assert len(extracted) == 1, "duplicate delivery emitted a second event"

    # And the same extracted event twice.
    for _ in range(2):
        async with sm() as session:
            async with session.begin():
                await handle_extracted(
                    session,
                    storage,
                    qdrant_client,
                    FakeEmbedder(dim=DIM),
                    FakeLLM(),
                    extracted[0],
                    collection=collection,
                    chunk_size=120,
                    chunk_overlap=24,
                )

    enriched = [
        e for e in await _drain_outbox(sm, "documents.enriched")
        if e.document_id == document_id
    ]
    assert len(enriched) == 1

    async with sm() as session:
        row = (await session.execute(select(Job).where(Job.id == job_id))).scalar_one()
    # Vectors were overwritten in place, not duplicated.
    assert (await qdrant_client.count(collection_name=collection)).count == row.chunk_count
