"""Kafka topic names and consumer groups for the DocStream pipeline.

One place that defines the event backbone so producers and consumers can never
drift. The pipeline is a chain:

    documents.ingested -> documents.extracted -> documents.enriched

Any handler that exhausts its retries routes the message to that topic's
dead-letter topic (``<topic>.DLQ``).
"""

from __future__ import annotations

# --- Pipeline topics (the happy path) ---
DOCUMENTS_INGESTED = "documents.ingested"
DOCUMENTS_EXTRACTED = "documents.extracted"
DOCUMENTS_ENRICHED = "documents.enriched"

PIPELINE_TOPICS: tuple[str, ...] = (
    DOCUMENTS_INGESTED,
    DOCUMENTS_EXTRACTED,
    DOCUMENTS_ENRICHED,
)

# --- Dead-letter suffix ---
DLQ_SUFFIX = ".DLQ"


def dlq_topic(topic: str) -> str:
    """Return the dead-letter topic name for a given pipeline topic.

    >>> dlq_topic(DOCUMENTS_INGESTED)
    'documents.ingested.DLQ'
    """
    if topic.endswith(DLQ_SUFFIX):
        return topic
    return f"{topic}{DLQ_SUFFIX}"


DLQ_TOPICS: tuple[str, ...] = tuple(dlq_topic(t) for t in PIPELINE_TOPICS)

# Every topic DocStream expects to exist locally.
ALL_TOPICS: tuple[str, ...] = PIPELINE_TOPICS + DLQ_TOPICS


# --- Consumer groups (one per worker role) ---
GROUP_EXTRACTION = "extraction-worker"
GROUP_ENRICHMENT = "enrichment-worker"
GROUP_QUERY_PROJECTOR = "query-projector"
