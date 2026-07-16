"""Smoke tests for the event contract: serialization, typing, DLQ helpers."""

from __future__ import annotations

from docstream.common.events import (
    DocumentIngested,
    EventEnvelope,
    EventType,
    make_event,
)
from docstream.common.topics import (
    ALL_TOPICS,
    DOCUMENTS_INGESTED,
    dlq_topic,
)


def _ingested() -> DocumentIngested:
    return DocumentIngested(
        job_id="job-1",
        document_id="doc-1",
        filename="lease.pdf",
        content_type="application/pdf",
        size_bytes=2048,
        storage_uri="local://uploads/doc-1.pdf",
    )


def test_make_event_wires_shared_fields() -> None:
    evt = make_event(_ingested(), source="api-gateway")
    assert evt.event_type is EventType.DOCUMENT_INGESTED
    assert evt.document_id == "doc-1"
    assert evt.source == "api-gateway"
    assert evt.attempt == 0
    assert evt.event_id and evt.correlation_id


def test_envelope_round_trips_through_bytes() -> None:
    evt = make_event(_ingested(), source="api-gateway")
    restored = EventEnvelope.from_bytes(evt.to_bytes())
    assert restored == evt
    # Payload keeps its concrete type via the discriminated union.
    assert isinstance(restored.payload, DocumentIngested)
    assert restored.payload.filename == "lease.pdf"


def test_partition_key_is_document_id() -> None:
    evt = make_event(_ingested(), source="api-gateway")
    assert evt.key() == b"doc-1"


def test_next_attempt_increments_and_preserves_identity() -> None:
    evt = make_event(_ingested(), source="api-gateway")
    retried = evt.next_attempt()
    assert retried.attempt == 1
    assert retried.event_id == evt.event_id  # same event, new delivery


def test_dlq_helpers() -> None:
    assert dlq_topic(DOCUMENTS_INGESTED) == "documents.ingested.DLQ"
    # Idempotent: already-DLQ stays put.
    assert dlq_topic("documents.ingested.DLQ") == "documents.ingested.DLQ"
    # 3 pipeline topics + 3 DLQ twins.
    assert len(ALL_TOPICS) == 6
