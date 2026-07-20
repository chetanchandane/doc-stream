"""Metrics: the counters that fire from the two shared chokepoints.

Instrumenting ``process_with_retry`` and ``mark_processed`` covers all three
workers at once, so these tests assert the plumbing there rather than in each
worker.
"""

from __future__ import annotations

from prometheus_client import REGISTRY

from docstream.common import metrics
from docstream.common.events import DocumentIngested, EventEnvelope, make_event
from docstream.common.retry import process_with_retry
from docstream.common.topics import DOCUMENTS_INGESTED
from docstream.db.idempotency import mark_processed


def _value(name: str, **labels) -> float:
    """Current value of a counter/gauge sample, 0 if it hasn't been touched."""
    return REGISTRY.get_sample_value(name, labels) or 0.0


def _envelope(attempt: int = 0) -> EventEnvelope:
    env = make_event(
        DocumentIngested(
            job_id="j",
            document_id="d",
            filename="f.txt",
            content_type="text/plain",
            size_bytes=1,
            storage_uri="mem://f",
        ),
        source="test",
    )
    return env.model_copy(update={"attempt": attempt})


async def _ok() -> None:
    return None


async def _boom() -> None:
    raise RuntimeError("nope")


# --------------------------------------------------------------------------- #
# Pipeline counters
# --------------------------------------------------------------------------- #
async def test_success_increments_success_and_observes_duration(producer):
    before = _value(
        "docstream_events_processed_total", stage=DOCUMENTS_INGESTED, result="success"
    )
    before_obs = _value(
        "docstream_event_processing_seconds_count", stage=DOCUMENTS_INGESTED
    )

    await process_with_retry(
        _ok,
        envelope=_envelope(),
        producer=producer,
        source_topic=DOCUMENTS_INGESTED,
        max_attempts=5,
    )

    after = _value(
        "docstream_events_processed_total", stage=DOCUMENTS_INGESTED, result="success"
    )
    after_obs = _value(
        "docstream_event_processing_seconds_count", stage=DOCUMENTS_INGESTED
    )
    assert after == before + 1
    assert after_obs == before_obs + 1


async def test_failure_increments_failed_and_retry(producer):
    before_failed = _value(
        "docstream_events_processed_total", stage=DOCUMENTS_INGESTED, result="failed"
    )
    before_retry = _value("docstream_retries_total", stage=DOCUMENTS_INGESTED)

    await process_with_retry(
        _boom,
        envelope=_envelope(attempt=0),
        producer=producer,
        source_topic=DOCUMENTS_INGESTED,
        max_attempts=5,
    )

    assert (
        _value(
            "docstream_events_processed_total",
            stage=DOCUMENTS_INGESTED,
            result="failed",
        )
        == before_failed + 1
    )
    assert _value("docstream_retries_total", stage=DOCUMENTS_INGESTED) == before_retry + 1


async def test_exhaustion_increments_dlq_not_retry(producer):
    before_dlq = _value("docstream_dlq_total", stage=DOCUMENTS_INGESTED)
    before_retry = _value("docstream_retries_total", stage=DOCUMENTS_INGESTED)

    # attempt=4 with max_attempts=5 -> dead-letter, not another retry.
    await process_with_retry(
        _boom,
        envelope=_envelope(attempt=4),
        producer=producer,
        source_topic=DOCUMENTS_INGESTED,
        max_attempts=5,
    )

    assert _value("docstream_dlq_total", stage=DOCUMENTS_INGESTED) == before_dlq + 1
    assert _value("docstream_retries_total", stage=DOCUMENTS_INGESTED) == before_retry


# --------------------------------------------------------------------------- #
# Dedup counter
# --------------------------------------------------------------------------- #
async def test_duplicate_event_increments_duplicate(sessionmaker):
    before = _value(
        "docstream_events_processed_total", stage="group-x", result="duplicate"
    )

    async with sessionmaker() as session:
        async with session.begin():
            assert await mark_processed(session, "evt-dup", "group-x") is True
            assert await mark_processed(session, "evt-dup", "group-x") is False

    assert (
        _value("docstream_events_processed_total", stage="group-x", result="duplicate")
        == before + 1
    )


# --------------------------------------------------------------------------- #
# Exposition
# --------------------------------------------------------------------------- #
def test_render_returns_prometheus_text():
    body, content_type = metrics.render()
    assert b"docstream_events_processed_total" in body
    assert "text/plain" in content_type


def test_metric_names_are_prefixed():
    """A shared prefix keeps our series distinguishable in a shared Prometheus."""
    body, _ = metrics.render()
    ours = [
        line
        for line in body.decode().splitlines()
        if line.startswith("# HELP ") and "docstream" in line
    ]
    assert ours, "expected docstream_* metrics in the exposition"
    for line in ours:
        assert line.split()[2].startswith("docstream_")
