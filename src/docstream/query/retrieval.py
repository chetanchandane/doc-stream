"""Retrieval: turn a natural-language question into relevant chunks.

Dense-only for now: embed the query with the same model used at index time, then
nearest-neighbour search in Qdrant. Query and index embeddings MUST come from the
same model or the vectors aren't comparable.

Upgrade path (ported from ClinRAG if recall becomes a problem): HyDE query
expansion, hybrid BM25 sparse vectors with RRF fusion, and Cohere reranking.
Deliberately left out here to keep the read path simple and debuggable.
"""

from __future__ import annotations

import logging

from docstream.enrichment.embedding import Embedder
from docstream.enrichment.qdrant_store import search

log = logging.getLogger("docstream.query.retrieval")


async def retrieve(
    *,
    embedder: Embedder,
    qdrant,
    collection: str,
    question: str,
    limit: int = 5,
    document_id: str | None = None,
) -> list[dict]:
    """Return the ``limit`` chunks most similar to ``question``.

    Each hit is ``{document_id, chunk_index, text, score}``. ``document_id``
    optionally restricts results to a single document (client-side filter — the
    payload carries document_id, so no Qdrant filter is needed at this scale).
    """
    if not question.strip():
        return []

    vectors = await embedder.embed([question])
    if not vectors:
        return []

    # Over-fetch when filtering so the filter doesn't starve the result set.
    fetch = limit * 4 if document_id else limit
    hits = await search(qdrant, collection, vectors[0], fetch)

    if document_id is not None:
        hits = [h for h in hits if h.get("document_id") == document_id]

    return hits[:limit]
