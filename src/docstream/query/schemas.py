"""Request/response models for the query service."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from docstream.db.models import DocumentView


class Chunk(BaseModel):
    """One retrieved excerpt."""

    document_id: str | None = None
    filename: str | None = None
    chunk_index: int | None = None
    text: str | None = None
    score: float | None = None


class SearchResponse(BaseModel):
    query: str
    count: int
    results: list[Chunk]


class AskRequest(BaseModel):
    question: str
    limit: int = 5
    document_id: str | None = None


class AskResponse(BaseModel):
    question: str
    answer: str
    # The excerpts the answer was grounded in — returned so callers (and you,
    # debugging) can see exactly what the model was given.
    sources: list[Chunk]


class DocumentSummary(BaseModel):
    """A row of the read model."""

    document_id: str
    job_id: str
    filename: str | None
    classification: str | None
    summary: str | None
    chunk_count: int
    indexed_at: datetime

    @classmethod
    def from_view(cls, view: DocumentView) -> "DocumentSummary":
        return cls(
            document_id=view.document_id,
            job_id=view.job_id,
            filename=view.filename,
            classification=view.classification,
            summary=view.summary,
            chunk_count=view.chunk_count,
            indexed_at=view.indexed_at,
        )
