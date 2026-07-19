"""Contract tests against a REAL Qdrant.

Every test here corresponds to an assumption the unit suite's fake could not
verify. Two of them are direct regressions for bugs that shipped:

1. ``upsert_chunks`` used raw "document_id:index" strings as point ids. Qdrant
   only accepts an unsigned integer or a UUID and rejected them with HTTP 400.
   The fake accepted any string, so the unit suite stayed green.

2. ``search`` called ``client.search(query_vector=...)``, which was deprecated
   and REMOVED from qdrant-client. The stub implemented that method, so again
   the unit suite passed while production raised AttributeError.

If either regresses, these fail here rather than at runtime.
"""

from __future__ import annotations

import uuid

import pytest

from docstream.enrichment.qdrant_store import (
    ensure_collection,
    point_id,
    search,
    upsert_chunks,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

DIM = 8


def _vec(axis: int) -> list[float]:
    """Unit vector along one axis, so different axes are ORTHOGONAL.

    Do NOT use ``[scalar] * DIM`` here. The collection uses COSINE distance,
    which normalizes away magnitude — so [0.9]*DIM and [0.1]*DIM are the same
    direction and score identically (1.0), making result ordering arbitrary.
    Distinct axes give genuinely distinguishable vectors.
    """
    v = [0.0] * DIM
    v[axis % DIM] = 1.0
    return v


async def test_ensure_collection_is_idempotent(qdrant_client):
    name = f"c_{uuid.uuid4().hex[:8]}"
    await ensure_collection(qdrant_client, name, DIM)
    # Calling twice must not raise (workers call it on every start).
    await ensure_collection(qdrant_client, name, DIM)
    assert await qdrant_client.collection_exists(name)


async def test_upsert_accepts_our_point_ids(qdrant_client):
    """REGRESSION: point ids must be valid Qdrant ids (int or UUID)."""
    name = f"c_{uuid.uuid4().hex[:8]}"
    await ensure_collection(qdrant_client, name, DIM)

    doc = str(uuid.uuid4())
    chunks = ["alpha text", "beta text", "gamma text"]
    vectors = [_vec(0), _vec(1), _vec(2)]

    ids = await upsert_chunks(qdrant_client, name, doc, chunks, vectors)

    assert len(ids) == 3
    # Every id we generate must parse as a UUID, or Qdrant rejects the upsert.
    for i in ids:
        uuid.UUID(i)
    assert ids[0] == point_id(doc, 0)

    count = (await qdrant_client.count(collection_name=name)).count
    assert count == 3


async def test_search_round_trip_returns_payload(qdrant_client):
    """REGRESSION: search must use the current query API and return payloads."""
    name = f"c_{uuid.uuid4().hex[:8]}"
    await ensure_collection(qdrant_client, name, DIM)

    doc = str(uuid.uuid4())
    chunks = ["the security deposit is 2000", "the lease term is 12 months"]
    vectors = [_vec(0), _vec(1)]
    await upsert_chunks(qdrant_client, name, doc, chunks, vectors)

    # Query along axis 0 -> chunk 0 is an exact direction match (cosine 1.0),
    # chunk 1 is orthogonal (0.0). Unambiguous ordering.
    hits = await search(qdrant_client, name, _vec(0), limit=2)

    assert len(hits) == 2
    top = hits[0]
    # The exact dict shape the query service depends on.
    assert set(top) == {"document_id", "chunk_index", "text", "score"}
    assert top["document_id"] == doc
    assert top["text"] == "the security deposit is 2000"
    assert isinstance(top["score"], float)
    # Sorted by score descending.
    assert hits[0]["score"] >= hits[1]["score"]


async def test_reupsert_overwrites_rather_than_duplicating(qdrant_client):
    """Deterministic ids are what make replayed events safe at the vector layer."""
    name = f"c_{uuid.uuid4().hex[:8]}"
    await ensure_collection(qdrant_client, name, DIM)

    doc = str(uuid.uuid4())
    chunks = ["one", "two"]
    vectors = [_vec(0), _vec(1)]

    first = await upsert_chunks(qdrant_client, name, doc, chunks, vectors)
    second = await upsert_chunks(qdrant_client, name, doc, chunks, vectors)

    assert first == second  # same ids both times
    count = (await qdrant_client.count(collection_name=name)).count
    assert count == 2  # not 4


async def test_search_limit_is_respected(qdrant_client):
    name = f"c_{uuid.uuid4().hex[:8]}"
    await ensure_collection(qdrant_client, name, DIM)

    doc = str(uuid.uuid4())
    chunks = [f"chunk {i}" for i in range(10)]
    vectors = [_vec(i) for i in range(10)]
    await upsert_chunks(qdrant_client, name, doc, chunks, vectors)

    hits = await search(qdrant_client, name, _vec(0), limit=3)
    assert len(hits) == 3
