"""Request/response models for the API Gateway."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from docstream.db.models import Job, JobStatus


class IngestResponse(BaseModel):
    job_id: str
    document_id: str
    status: JobStatus


class JobResponse(BaseModel):
    job_id: str
    document_id: str
    filename: str
    content_type: str
    size_bytes: int
    status: JobStatus
    error: str | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_job(cls, job: Job) -> "JobResponse":
        return cls(
            job_id=job.id,
            document_id=job.document_id,
            filename=job.filename,
            content_type=job.content_type,
            size_bytes=job.size_bytes,
            status=job.status,
            error=job.error,
            created_at=job.created_at,
            updated_at=job.updated_at,
        )
