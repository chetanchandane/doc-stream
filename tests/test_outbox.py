"""The transactional-outbox write path and the relay drain logic."""

from __future__ import annotations

from sqlalchemy import func, select

from docstream.common.events import DocumentIngested, EventEnvelope, EventType
from docstream.db.models import Job, JobStatus, OutboxEvent
from docstream.db.outbox import fetch_unpublished
from docstream.gateway.relay import drain_once
from docstream.gateway.service import create_ingestion_job


async def test_create_ingestion_job_writes_job_and_outbox_atomically(sessionmaker):
    async with sessionmaker() as session:
        async with session.begin():
            job = await create_ingestion_job(
                session,
                filename="lease.pdf",
                content_type="application/pdf",
                size_bytes=1234,
                storage_uri="file:///tmp/doc.pdf",
            )
        job_id, document_id = job.id, job.document_id

    async with sessionmaker() as session:
        jobs = (await session.execute(select(func.count()).select_from(Job))).scalar()
        outbox = (
            await session.execute(select(func.count()).select_from(OutboxEvent))
        ).scalar()
        assert jobs == 1 and outbox == 1

        row = (await session.execute(select(OutboxEvent))).scalar_one()
        assert row.topic == "documents.ingested"
        assert row.aggregate_id == document_id
        assert row.key == document_id
        assert row.published_at is None

        envelope = EventEnvelope.from_bytes(row.payload.encode())
        assert envelope.event_type is EventType.DOCUMENT_INGESTED
        assert isinstance(envelope.payload, DocumentIngested)
        assert envelope.payload.job_id == job_id


async def test_drain_once_publishes_and_marks(sessionmaker, producer):
    async with sessionmaker() as session:
        async with session.begin():
            await create_ingestion_job(
                session,
                filename="a.pdf",
                content_type="application/pdf",
                size_bytes=1,
                storage_uri="file:///tmp/a.pdf",
            )

    async with sessionmaker() as session:
        count = await drain_once(session, producer, batch_size=10)

    assert count == 1
    assert len(producer.published) == 1
    topic, value, key = producer.published[0]
    assert topic == "documents.ingested"
    envelope = EventEnvelope.from_bytes(value)
    assert key == envelope.document_id.encode()

    # Row is now marked published, so a second drain does nothing.
    async with sessionmaker() as session:
        remaining = await fetch_unpublished(session, 10)
        assert remaining == []
        again = await drain_once(session, producer, batch_size=10)
    assert again == 0
    assert len(producer.published) == 1


async def test_drain_once_empty_returns_zero(sessionmaker, producer):
    async with sessionmaker() as session:
        assert await drain_once(session, producer, batch_size=10) == 0
    assert producer.published == []


async def test_job_defaults_to_pending(sessionmaker):
    async with sessionmaker() as session:
        async with session.begin():
            job = await create_ingestion_job(
                session,
                filename="x.txt",
                content_type="text/plain",
                size_bytes=3,
                storage_uri="file:///tmp/x.txt",
            )
        assert job.status is JobStatus.PENDING
