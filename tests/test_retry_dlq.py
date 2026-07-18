"""Retry + DLQ policy: process_with_retry re-publishes or dead-letters.

Exercises the wrapper in isolation with the FakeProducer fixture — no Kafka.
"""

from __future__ import annotations

from docstream.common.events import DocumentIngested, EventEnvelope, make_event
from docstream.common.retry import process_with_retry
from docstream.common.topics import DOCUMENTS_INGESTED, dlq_topic


def _envelope(attempt: int = 0) -> EventEnvelope:
    env = make_event(
        DocumentIngested(
            job_id="j1",
            document_id="d1",
            filename="f.txt",
            content_type="text/plain",
            size_bytes=3,
            storage_uri="mem://f",
        ),
        source="test",
    )
    return env.model_copy(update={"attempt": attempt})


async def _ok() -> None:
    return None


def _boom_factory():
    async def _boom() -> None:
        raise RuntimeError("handler failed")

    return _boom


async def test_success_publishes_nothing(producer):
    await process_with_retry(
        _ok,
        envelope=_envelope(),
        producer=producer,
        source_topic=DOCUMENTS_INGESTED,
        max_attempts=5,
    )
    assert producer.published == []


async def test_failure_requeues_with_incremented_attempt(producer):
    env = _envelope(attempt=0)
    await process_with_retry(
        _boom_factory(),
        envelope=env,
        producer=producer,
        source_topic=DOCUMENTS_INGESTED,
        max_attempts=5,
    )
    # One publish, back to the SAME topic, attempt incremented to 1.
    assert len(producer.published) == 1
    topic, value, key = producer.published[0]
    assert topic == DOCUMENTS_INGESTED
    assert key == env.key()
    requeued = EventEnvelope.from_bytes(value)
    assert requeued.attempt == 1
    assert requeued.event_id == env.event_id  # same event, so dedup claim reruns


async def test_exhausted_routes_to_dlq_and_runs_on_dlq(producer):
    # attempt=4, max_attempts=5 -> next_attempt=5 >= 5 -> DLQ.
    env = _envelope(attempt=4)
    dlq_called: list[BaseException] = []

    async def _on_dlq(exc: BaseException) -> None:
        dlq_called.append(exc)

    await process_with_retry(
        _boom_factory(),
        envelope=env,
        producer=producer,
        source_topic=DOCUMENTS_INGESTED,
        max_attempts=5,
        on_dlq=_on_dlq,
    )

    assert len(producer.published) == 1
    topic, value, key = producer.published[0]
    assert topic == dlq_topic(DOCUMENTS_INGESTED) == "documents.ingested.DLQ"
    assert key == env.key()
    # on_dlq ran with the original exception.
    assert len(dlq_called) == 1
    assert isinstance(dlq_called[0], RuntimeError)


async def test_retries_then_dlq_across_the_full_ladder(producer):
    """Walk attempt 0..4: four requeues, then the fifth failure dead-letters."""
    max_attempts = 5
    published_topics: list[str] = []

    # Simulate the sequence of deliveries the broker would make.
    for attempt in range(max_attempts):
        p = producer.__class__()  # fresh recorder per delivery
        await process_with_retry(
            _boom_factory(),
            envelope=_envelope(attempt=attempt),
            producer=p,
            source_topic=DOCUMENTS_INGESTED,
            max_attempts=max_attempts,
        )
        published_topics.append(p.published[0][0])

    assert published_topics[:4] == [DOCUMENTS_INGESTED] * 4
    assert published_topics[4] == dlq_topic(DOCUMENTS_INGESTED)
