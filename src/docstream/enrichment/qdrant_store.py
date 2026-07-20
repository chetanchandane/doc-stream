"""
qdrant_store.py — Qdrant collection management, upsert, and search.

Ported from ClinRAG src/ingestion/indexer.py (_ensure_collection, _upload,
_already_indexed) and src/retrieval/search.py. Two deliberate departures from
ClinRAG:

  1. DETERMINISTIC POINT IDS. ClinRAG assigns id=uuid.uuid4() per chunk and
     dedups at the FILE level by scrolling on the `source` payload. That does
     not survive event replays. DocStream uses `f"{document_id}:{i}"` so a
     replayed `documents.extracted` event overwrites the same points in place
     instead of duplicating them. This is what makes Phase 3 idempotency safe
     at the vector layer.

  2. SINGLE UNNAMED DENSE VECTOR. ClinRAG builds a named dense+sparse schema for
     hybrid BM25 search. The DocStream MVP is dense-only; add sparse later if the
     Query API needs better recall (see week2-plan.md section 10).

All functions take an AsyncQdrantClient so the worker owns the client lifecycle.
"""

from __future__ import annotations

import uuid

from qdrant_client import AsyncQdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

from docstream.common import metrics

# Fixed namespace so the derived UUIDs are stable across processes and runs.
_POINT_NAMESPACE = uuid.UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff")


def point_id(document_id: str, index: int) -> str:
    """Deterministic Qdrant point id for chunk `index` of `document_id`.

    Qdrant only accepts point ids that are an unsigned integer OR a UUID, so we
    can't use a raw `f"{document_id}:{index}"` string. Instead we derive a stable
    UUIDv5 from it: same (document_id, index) always yields the same UUID, so a
    replayed event overwrites the same points in place (idempotent) rather than
    duplicating them.
    """
    return str(uuid.uuid5(_POINT_NAMESPACE, f"{document_id}:{index}"))


async def ensure_collection(
    client: AsyncQdrantClient,
    collection: str,
    vector_size: int,
    distance: Distance = Distance.COSINE,
) -> None:
    """
    Create the collection if it does not already exist. Idempotent: safe to call
    on every worker start. Unlike ClinRAG this does not drop/recreate on schema
    mismatch — DocStream owns the collection, so a mismatch is a real error.
    """
    if await client.collection_exists(collection):
        return
    await client.create_collection(
        collection_name=collection,
        vectors_config=VectorParams(size=vector_size, distance=distance),
    )


async def upsert_chunks(
    client: AsyncQdrantClient,
    collection: str,
    document_id: str,
    chunks: list[str],
    vectors: list[list[float]],
    batch_size: int = 200,
) -> list[str]:
    """
    Upsert one point per chunk with a deterministic id. Re-running with the same
    document_id overwrites the same points (idempotent).

    Returns the list of point ids written, in chunk order — the worker puts these
    in DocumentEnriched.vector_ids.
    """
    if len(chunks) != len(vectors):
        raise ValueError(f"chunks/vectors length mismatch: {len(chunks)} vs {len(vectors)}")

    ids = [point_id(document_id, i) for i in range(len(chunks))]
    points = [
        PointStruct(
            id=ids[i],
            vector=vectors[i],
            payload={"document_id": document_id, "chunk_index": i, "text": chunks[i]},
        )
        for i in range(len(chunks))
    ]

    for start in range(0, len(points), batch_size):
        with metrics.timed_call("qdrant", "upsert"):
            await client.upsert(
                collection_name=collection,
                points=points[start : start + batch_size],
            )

    return ids


async def search(
    client: AsyncQdrantClient,
    collection: str,
    query_vector: list[float],
    limit: int = 5,
) -> list[dict]:
    """
    Dense similarity search for the Query API. Returns a list of
    {document_id, chunk_index, text, score} dicts sorted by score descending.

    Uses ``query_points`` — the modern unified query API. The older
    ``client.search(query_vector=...)`` was deprecated and REMOVED in current
    qdrant-client versions, so calling it raises AttributeError.
    """
    with metrics.timed_call("qdrant", "search"):
        response = await client.query_points(
            collection_name=collection,
            query=query_vector,
            limit=limit,
            with_payload=True,
        )
    results: list[dict] = []
    for hit in response.points:
        payload = hit.payload or {}
        results.append(
            {
                "document_id": payload.get("document_id"),
                "chunk_index": payload.get("chunk_index"),
                "text": payload.get("text"),
                "score": hit.score,
            }
        )
    return results
