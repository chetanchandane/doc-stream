"""Event schemas for the DocStream pipeline.

Every message on the bus is an :class:`EventEnvelope`. The envelope carries the
metadata that the distributed-systems patterns rely on:

* ``event_id``     -- stable UUID used for idempotent consumption (dedup key).
* ``event_type``   -- discriminator, matches the topic's payload type.
* ``occurred_at``  -- when the producer created the event.
* ``document_id``  -- the aggregate this event belongs to (partition key).
* ``correlation_id`` -- ties every event for one document together (tracing).
* ``attempt``      -- retry counter; the DLQ policy reads this.
* ``payload``      -- the typed body, one of the ``Document*`` models below.

Keep this module free of I/O so it can be imported by any service and unit
tested in isolation.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Literal, TypeVar, Union

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _new_uuid() -> str:
    return str(uuid.uuid4())


class EventType(str, Enum):
    """Canonical event type strings. Values line up with topic payloads."""

    DOCUMENT_INGESTED = "document.ingested"
    DOCUMENT_EXTRACTED = "document.extracted"
    DOCUMENT_ENRICHED = "document.enriched"


# --------------------------------------------------------------------------- #
# Payloads: one per stage of the pipeline.
# --------------------------------------------------------------------------- #
class DocumentIngested(BaseModel):
    """Emitted by the API Gateway once a job row is persisted."""

    event_type: Literal[EventType.DOCUMENT_INGESTED] = EventType.DOCUMENT_INGESTED
    job_id: str
    document_id: str
    filename: str
    content_type: str
    size_bytes: int
    storage_uri: str = Field(
        description="Where the raw bytes live (object store key or local path)."
    )


class DocumentExtracted(BaseModel):
    """Emitted by the Extraction Worker after pulling text (OCR if needed)."""

    event_type: Literal[EventType.DOCUMENT_EXTRACTED] = EventType.DOCUMENT_EXTRACTED
    job_id: str
    document_id: str
    text_uri: str = Field(description="Where the extracted plain text is stored.")
    text_length: int
    page_count: int = 0
    extraction_method: str = Field(
        default="native", description="e.g. 'native', 'ocr', 'hybrid'."
    )


class DocumentEnriched(BaseModel):
    """Emitted by the AI Enrichment Worker after embedding + LLM enrichment."""

    event_type: Literal[EventType.DOCUMENT_ENRICHED] = EventType.DOCUMENT_ENRICHED
    job_id: str
    document_id: str
    classification: str | None = None
    summary: str | None = None
    chunk_count: int = 0
    vector_ids: list[str] = Field(default_factory=list)


# Discriminated union of every payload type. Pydantic uses ``event_type`` to
# pick the right model when parsing an unknown envelope off the wire.
Payload = Annotated[
    Union[DocumentIngested, DocumentExtracted, DocumentEnriched],
    Field(discriminator="event_type"),
]

P = TypeVar("P", DocumentIngested, DocumentExtracted, DocumentEnriched)


class EventEnvelope(BaseModel):
    """Uniform wrapper for every message on the bus."""

    event_id: str = Field(default_factory=_new_uuid)
    event_type: EventType
    occurred_at: datetime = Field(default_factory=_utcnow)
    document_id: str
    correlation_id: str = Field(default_factory=_new_uuid)
    attempt: int = 0
    source: str = Field(default="unknown", description="Service that produced the event.")
    payload: Payload

    def key(self) -> bytes:
        """Kafka partition key: keep one document's events on one partition
        so ordering per document is preserved."""
        return self.document_id.encode("utf-8")

    def to_bytes(self) -> bytes:
        """Serialize for the Kafka value."""
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> EventEnvelope:
        """Parse a Kafka value back into a typed envelope."""
        return cls.model_validate_json(raw)

    def next_attempt(self) -> EventEnvelope:
        """Return a copy with the retry counter incremented (used by DLQ logic)."""
        return self.model_copy(update={"attempt": self.attempt + 1})


def make_event(
    payload: DocumentIngested | DocumentExtracted | DocumentEnriched,
    *,
    source: str,
    correlation_id: str | None = None,
) -> EventEnvelope:
    """Build an envelope from a payload, wiring the shared fields for you."""
    kwargs: dict[str, object] = {
        "event_type": payload.event_type,
        "document_id": payload.document_id,
        "source": source,
        "payload": payload,
    }
    if correlation_id is not None:
        kwargs["correlation_id"] = correlation_id
    return EventEnvelope(**kwargs)
