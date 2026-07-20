"""Retry + dead-letter wrapper for worker message handling.

``process_with_retry`` runs a unit of work and, on failure, decides whether to
retry or dead-letter — so every worker gets the same fault-tolerance policy
without duplicating the logic.

Policy (Decision 3 in week2-plan.md: same-topic re-publish):

* success                          -> nothing extra; caller commits the offset.
* failure, attempts left           -> re-publish the event to its OWN topic with
                                      ``attempt`` incremented; it gets consumed
                                      and retried later.
* failure, attempts exhausted      -> publish the event to ``<topic>.DLQ`` and
                                      run ``on_dlq`` (mark the job failed).

Retries and DLQ go through the Kafka producer DIRECTLY, not the outbox: they are
operational redeliveries, not new business state. The work itself owns its own
DB transaction, so a failed attempt has already rolled back (including the
idempotency claim), leaving the event safe to reprocess.

Limitation: backoff is a sleep in the consumer loop, so it delays that
partition. Fine for the MVP; the upgrade path is a dedicated retry topic with a
delay consumer.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from docstream.common import metrics
from docstream.common.events import EventEnvelope
from docstream.common.topics import dlq_topic

if TYPE_CHECKING:
    # Only needed for the type hint; the function duck-types on `.publish()`, so
    # we avoid importing the aiokafka-backed producer (and its dependency) at
    # runtime. Tests can pass any object with an async publish(topic, value, key).
    from docstream.common.messaging import KafkaProducer

log = logging.getLogger("docstream.retry")


async def process_with_retry(
    work: Callable[[], Awaitable[None]],
    *,
    envelope: EventEnvelope,
    producer: KafkaProducer,
    source_topic: str,
    max_attempts: int,
    backoff_seconds: float = 0.0,
    on_dlq: Callable[[BaseException], Awaitable[None]] | None = None,
) -> None:
    """Run ``work``; retry via ``source_topic`` or dead-letter on exhaustion.

    ``max_attempts`` is the total number of processing attempts (the initial
    delivery counts as attempt 0). The caller commits the Kafka offset after this
    returns, regardless of outcome — the retry lives on as a new message.
    """
    # Every worker funnels through here, so instrumenting once covers all three.
    # Timing wraps only the handler, not the retry/DLQ publishing that follows,
    # so the histogram measures work rather than error handling.
    started = time.perf_counter()
    try:
        await work()
        metrics.event_processing_seconds.labels(stage=source_topic).observe(
            time.perf_counter() - started
        )
        metrics.events_processed_total.labels(stage=source_topic, result="success").inc()
        return
    except Exception as exc:  # noqa: BLE001 - policy decision, not silent swallow
        metrics.event_processing_seconds.labels(stage=source_topic).observe(
            time.perf_counter() - started
        )
        metrics.events_processed_total.labels(stage=source_topic, result="failed").inc()
        next_attempt = envelope.attempt + 1

        if next_attempt >= max_attempts:
            metrics.dlq_total.labels(stage=source_topic).inc()
            dlq = dlq_topic(source_topic)
            await producer.publish(dlq, envelope.to_bytes(), envelope.key())
            log.error(
                "event %s exhausted %d attempts; routed to %s (%r)",
                envelope.event_id,
                max_attempts,
                dlq,
                exc,
            )
            if on_dlq is not None:
                await on_dlq(exc)
            return

        metrics.retries_total.labels(stage=source_topic).inc()

        if backoff_seconds:
            await asyncio.sleep(backoff_seconds * next_attempt)

        retried = envelope.next_attempt()
        await producer.publish(source_topic, retried.to_bytes(), retried.key())
        log.warning(
            "event %s failed (attempt %d/%d); requeued to %s (%r)",
            envelope.event_id,
            next_attempt,
            max_attempts,
            source_topic,
            exc,
        )
