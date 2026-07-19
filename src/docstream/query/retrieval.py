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


def filter_by_relevance(
    hits: list[dict], *, min_score: float = 0.0, relative_cutoff: float = 0.0
) -> list[dict]:
    """Drop weak matches from a ranked hit list.

    Vector search returns the top-K nearest neighbours no matter how distant, so
    a specific question against a small corpus returns unrelated chunks padding
    out the limit. Two complementary cutoffs:

    * ``min_score`` — an absolute floor; anything below is simply not relevant.
    * ``relative_cutoff`` — a fraction of the best hit's score. Adapts to the
      query: when the top hit scores 0.9 a 0.3 match is noise, but when the top
      hit scores 0.35 a 0.30 match may be genuine.

    Either can be disabled by passing 0. Assumes ``hits`` is sorted descending.
    """
    if not hits:
        return []

    kept = [h for h in hits if (h.get("score") or 0.0) >= min_score]

    if relative_cutoff > 0 and kept:
        best = kept[0].get("score") or 0.0
        threshold = best * relative_cutoff
        kept = [h for h in kept if (h.get("score") or 0.0) >= threshold]

    return kept


async def retrieve(
    *,
    embedder: Embedder,
    qdrant,
    collection: str,
    question: str,
    limit: int = 5,
    document_id: str | None = None,
    min_score: float = 0.0,
    relative_cutoff: float = 0.0,
) -> list[dict]:
    """Return up to ``limit`` chunks relevant to ``question``.

    Each hit is ``{document_id, chunk_index, text, score}``. ``document_id``
    optionally restricts results to a single document (client-side filter — the
    payload carries document_id, so no Qdrant filter is needed at this scale).

    Returns FEWER than ``limit`` when only a few chunks clear the relevance
    cutoffs, and an empty list when nothing does — which is the honest answer,
    and lets the generator say it doesn't know instead of citing noise.
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

    hits = filter_by_relevance(
        hits, min_score=min_score, relative_cutoff=relative_cutoff
    )
    if not hits:
        log.info("no chunks cleared the relevance cutoff for query %r", question)

    return hits[:limit]
