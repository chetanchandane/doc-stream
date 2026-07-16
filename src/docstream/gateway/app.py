"""FastAPI API Gateway.

Endpoints:
    POST /documents      ingest a document (multipart file upload)
    GET  /jobs/{job_id}  fetch job status
    GET  /healthz        liveness

On startup it optionally launches the outbox relay as a background task so a
single ``uvicorn`` process runs the whole write path locally.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

from fastapi import Depends, FastAPI, HTTPException, UploadFile
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docstream.common.config import get_settings
from docstream.db.base import get_session
from docstream.db.models import Job
from docstream.gateway.schemas import IngestResponse, JobResponse
from docstream.gateway.service import create_ingestion_job
from docstream.storage import get_storage


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    relay_task: asyncio.Task[None] | None = None
    if settings.relay.run_in_process:
        # Imported lazily so tests can drive the relay directly without Kafka.
        from docstream.gateway.relay import run_relay

        relay_task = asyncio.create_task(run_relay())
    try:
        yield
    finally:
        if relay_task is not None:
            relay_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await relay_task


app = FastAPI(title="DocStream API Gateway", version="0.1.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/documents", response_model=IngestResponse, status_code=202)
async def ingest_document(
    file: UploadFile,
    session: AsyncSession = Depends(get_session),
) -> IngestResponse:
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Empty file.")

    storage = get_storage()
    # A provisional id purely for the storage path; the real document_id is
    # minted inside the transaction. We save bytes first, then record the job.
    import uuid

    provisional_id = str(uuid.uuid4())
    storage_uri = storage.save(
        provisional_id, file.filename or "document.bin", data
    )

    async with session.begin():
        job = await create_ingestion_job(
            session,
            filename=file.filename or "document.bin",
            content_type=file.content_type or "application/octet-stream",
            size_bytes=len(data),
            storage_uri=storage_uri,
        )
    return IngestResponse(
        job_id=job.id, document_id=job.document_id, status=job.status
    )


@app.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: str,
    session: AsyncSession = Depends(get_session),
) -> JobResponse:
    result = await session.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found.")
    return JobResponse.from_job(job)
