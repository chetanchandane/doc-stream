"""Prometheus metrics for the DocStream pipeline.

Deliberately a small, opinionated set. Metrics are cheap to add and expensive to
maintain, so these answer specific operational questions rather than measuring
everything measurable:

* Is the pipeline moving?            -> events_processed_total{stage,result}
* Which stage is slow?               -> event_processing_seconds{stage}
* Is it retrying or dead-lettering?  -> retries_total / dlq_total
* Is redelivery being deduped?       -> result="duplicate" on the counter above
* Is the outbox draining?            -> outbox_pending
* Are the APIs healthy?              -> http_requests_total / http_request_seconds

The counters are incremented in the two places every worker already funnels
through — ``common/retry.py`` and ``db/idempotency.py`` — so all three workers
are instrumented without touching any of them individually.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #
events_processed_total = Counter(
    "docstream_events_processed_total",
    "Events consumed, by pipeline stage and outcome.",
    ["stage", "result"],  # result: success | duplicate | failed
)

event_processing_seconds = Histogram(
    "docstream_event_processing_seconds",
    "Wall-clock time to handle one event, by stage.",
    ["stage"],
    # Enrichment calls an embedding API and an LLM, so the useful range spans
    # milliseconds (projection) to tens of seconds (enrichment).
    buckets=(0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60),
)

retries_total = Counter(
    "docstream_retries_total",
    "Events re-published to their own topic after a handler failure.",
    ["stage"],
)

dlq_total = Counter(
    "docstream_dlq_total",
    "Events routed to a dead-letter topic after exhausting retries.",
    ["stage"],
)

# --------------------------------------------------------------------------- #
# Write path
# --------------------------------------------------------------------------- #
documents_ingested_total = Counter(
    "docstream_documents_ingested_total",
    "Documents accepted by the API gateway.",
)

outbox_pending = Gauge(
    "docstream_outbox_pending",
    "Outbox rows not yet published to Kafka. Sustained growth means the relay "
    "is falling behind or wedged.",
)

# --------------------------------------------------------------------------- #
# External calls — where the seconds and the money actually go.
#
# event_processing_seconds{stage="documents.extracted"} covers chunk + embed +
# upsert + LLM as one opaque number. Splitting it by provider/operation is what
# turns "enrichment is slow" into "the embedding API is slow".
# --------------------------------------------------------------------------- #
external_call_seconds = Histogram(
    "docstream_external_call_seconds",
    "Latency of calls to external services (embedding, LLM, vector store).",
    ["provider", "operation"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 45),
)

external_calls_total = Counter(
    "docstream_external_calls_total",
    "External service calls, by outcome — a rising error rate here explains "
    "retries and DLQ traffic upstream.",
    ["provider", "operation", "result"],  # result: success | error
)

# --------------------------------------------------------------------------- #
# Retrieval quality
#
# Note this is deliberately NOT a cache-style hit/miss: a vector search always
# returns k neighbours. What matters is HOW relevant they are, so we record the
# top hit's similarity score. A distribution drifting toward zero means the
# corpus no longer answers what people ask.
# --------------------------------------------------------------------------- #
retrieval_top_score = Histogram(
    "docstream_retrieval_top_score",
    "Similarity score of the best hit per query (0-1, cosine).",
    buckets=(0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0),
)

retrieval_chunks_returned = Histogram(
    "docstream_retrieval_chunks_returned",
    "Chunks surviving the relevance cutoff per query. Zero means the model was "
    "asked to decline rather than cite noise.",
    buckets=(0, 1, 2, 3, 5, 10, 20, 50),
)

# --------------------------------------------------------------------------- #
# Read path
# --------------------------------------------------------------------------- #
queries_total = Counter(
    "docstream_queries_total",
    "Read-side queries, by kind and whether anything relevant was retrieved.",
    ["kind", "result"],  # kind: search | ask ; result: hit | empty
)

# --------------------------------------------------------------------------- #
# HTTP (both FastAPI services)
# --------------------------------------------------------------------------- #
http_requests_total = Counter(
    "docstream_http_requests_total",
    "HTTP requests handled.",
    ["service", "method", "path", "status"],
)

http_request_seconds = Histogram(
    "docstream_http_request_seconds",
    "HTTP request duration.",
    ["service", "method", "path"],
    buckets=(0.005, 0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30),
)


@contextmanager
def timed_call(provider: str, operation: str) -> Iterator[None]:
    """Time an external call and record success/failure.

    Used to wrap the real provider clients only — the Fake* implementations stay
    uninstrumented so test runs don't pollute latency histograms with numbers
    that mean nothing.
    """
    started = time.perf_counter()
    result = "success"
    try:
        yield
    except Exception:
        result = "error"
        raise
    finally:
        elapsed = time.perf_counter() - started
        external_call_seconds.labels(provider=provider, operation=operation).observe(elapsed)
        external_calls_total.labels(
            provider=provider, operation=operation, result=result
        ).inc()


def render() -> tuple[bytes, str]:
    """Return ``(body, content_type)`` for a /metrics response."""
    return generate_latest(), CONTENT_TYPE_LATEST
