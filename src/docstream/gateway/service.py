"""Ingestion service: the transactional-outbox write path.

``create_ingestion_job`` writes the ``jobs`` row and its ``documents.ingested``
outbox row in a single transaction. Either both land or neither does, so a job
can never exist without its event queued (and no event is queued for a job that
was rolled back).
"""

from __future__ import annotations

import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from docstream.common.events import DocumentIngested, make_event
from docstream.db.models import Job, JobStatus
from docstream.db.outbox import enqueue_event

SOURCE = "api-gateway"


async def create_ingestion_job(
    session: AsyncSession,
    *,
    filename: str,
    content_type: str,
    size_bytes: int,
    storage_uri: str,
) -> Job:
    """Persist a job and stage its ingested event atomically.

    The caller is responsible for committing the surrounding transaction.
    """
    job_id = str(uuid.uuid4())
    document_id = str(uuid.uuid4())
    job = Job(
        id=job_id,
        document_id=document_id,
        filename=filename,
        content_type=content_type,
        size_bytes=size_bytes,
        storage_uri=storage_uri,
        status=JobStatus.PENDING,
    )
    session.add(job)

    envelope = make_event(
        DocumentIngested(
            job_id=job_id,
            document_id=document_id,
            filename=filename,
            content_type=content_type,
            size_bytes=size_bytes,
            storage_uri=storage_uri,
        ),
        source=SOURCE,
    )
    enqueue_event(session, envelope)
    return job
