"""Extraction: the pure text function and the handler's DB + outbox effects."""

from __future__ import annotations

from sqlalchemy import select

from docstream.common.events import (
    DocumentExtracted,
    EventEnvelope,
    EventType,
    make_event,
)
from docstream.common.events import DocumentIngested
from docstream.db.models import Job, JobStatus, OutboxEvent
from docstream.extraction.text import extract_text
from docstream.extraction.worker import handle_ingested
from docstream.gateway.service import create_ingestion_job
from docstream.storage.local import LocalStorage


def test_extract_text_plain():
    result = extract_text("notes.txt", "text/plain", b"hello world")
    assert result.text == "hello world"
    assert result.method == "native"
    assert result.page_count == 1


def test_extract_text_unknown_type_falls_back():
    result = extract_text("blob.bin", "application/octet-stream", b"raw bytes")
    assert result.text == "raw bytes"
    assert result.method == "fallback"


async def test_handle_ingested_extracts_and_emits(sessionmaker, tmp_path):
    storage = LocalStorage(tmp_path / "store")

    # Seed a job the way the gateway would, and put the raw bytes in storage.
    async with sessionmaker() as session:
        async with session.begin():
            job = await create_ingestion_job(
                session,
                filename="lease.txt",
                content_type="text/plain",
                size_bytes=11,
                storage_uri="",  # replaced below
            )
        document_id, job_id = job.document_id, job.id

    storage_uri = storage.save(document_id, "lease.txt", b"lease terms")
    async with sessionmaker() as session:
        async with session.begin():
            j = (
                await session.execute(select(Job).where(Job.id == job_id))
            ).scalar_one()
            j.storage_uri = storage_uri

    # Build the ingested envelope the worker would receive.
    envelope = make_event(
        DocumentIngested(
            job_id=job_id,
            document_id=document_id,
            filename="lease.txt",
            content_type="text/plain",
            size_bytes=11,
            storage_uri=storage_uri,
        ),
        source="api-gateway",
    )

    async with sessionmaker() as session:
        async with session.begin():
            await handle_ingested(session, storage, envelope)

    async with sessionmaker() as session:
        job = (await session.execute(select(Job).where(Job.id == job_id))).scalar_one()
        assert job.status is JobStatus.EXTRACTED

        # A documents.extracted event is now staged in the outbox.
        rows = (
            await session.execute(
                select(OutboxEvent).where(OutboxEvent.topic == "documents.extracted")
            )
        ).scalars().all()
        assert len(rows) == 1
        extracted = EventEnvelope.from_bytes(rows[0].payload.encode())
        assert extracted.event_type is EventType.DOCUMENT_EXTRACTED
        assert isinstance(extracted.payload, DocumentExtracted)
        assert extracted.payload.text_length == len("lease terms")
        assert extracted.payload.extraction_method == "native"
        # Correlation id is preserved for tracing across the two events.
        assert extracted.correlation_id == envelope.correlation_id


async def test_handle_ingested_unknown_job_is_noop(sessionmaker, tmp_path):
    storage = LocalStorage(tmp_path / "store")
    envelope = make_event(
        DocumentIngested(
            job_id="ghost",
            document_id="missing-doc",
            filename="x.txt",
            content_type="text/plain",
            size_bytes=1,
            storage_uri="file:///nope.txt",
        ),
        source="api-gateway",
    )
    async with sessionmaker() as session:
        async with session.begin():
            await handle_ingested(session, storage, envelope)
        # No outbox row created for a missing job.
        count = (
            await session.execute(select(OutboxEvent))
        ).scalars().all()
    assert count == []
