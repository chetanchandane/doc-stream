"""Query service (read side): read-model listing, search, and grounded answers.

All fakes — no Qdrant, no OpenAI, no Anthropic.
"""

from __future__ import annotations

from docstream.common.events import DocumentEnriched, make_event
from docstream.enrichment.embedding import FakeEmbedder
from docstream.projection.worker import handle_enriched
from docstream.query import service
from docstream.query.generation import FakeGenerator
from docstream.query.prompts import build_user_message


class _ScoredPoint:
    """Mirrors qdrant_client ScoredPoint (the fields we read)."""

    def __init__(self, payload: dict, score: float):
        self.payload = payload
        self.score = score


class _QueryResponse:
    """Mirrors qdrant_client QueryResponse: results live under .points."""

    def __init__(self, points: list[_ScoredPoint]):
        self.points = points


class StubQdrant:
    """Returns canned hits regardless of the query vector.

    Deliberately mirrors the REAL client surface: ``query_points(...) ->
    QueryResponse`` with a ``.points`` list. An earlier version of this stub
    implemented the long-removed ``search()`` method, so the tests passed while
    production broke with AttributeError. Fakes must track the real API.
    """

    def __init__(self, hits: list[dict] | None = None):
        self._hits = hits if hits is not None else []
        self.last_limit: int | None = None

    async def query_points(
        self, collection_name, query, limit=10, with_payload=True, **kwargs
    ):
        self.last_limit = limit
        return _QueryResponse(
            [
                _ScoredPoint(
                    {
                        "document_id": h["document_id"],
                        "chunk_index": h["chunk_index"],
                        "text": h["text"],
                    },
                    h["score"],
                )
                for h in self._hits[:limit]
            ]
        )


async def _project(sessionmaker, document_id: str, **overrides):
    payload = dict(
        job_id=f"job-{document_id}",
        document_id=document_id,
        filename=f"{document_id}.pdf",
        classification="resume",
        summary="s",
        chunk_count=3,
        vector_ids=[],
    )
    payload.update(overrides)
    async with sessionmaker() as session:
        async with session.begin():
            await handle_enriched(
                session, make_event(DocumentEnriched(**payload), source="test")
            )


# --------------------------------------------------------------------------- #
# Read model
# --------------------------------------------------------------------------- #
async def test_list_and_get_documents(sessionmaker):
    await _project(sessionmaker, "doc-1")
    await _project(sessionmaker, "doc-2", classification="invoice")

    async with sessionmaker() as session:
        all_docs = await service.list_documents(session)
        assert len(all_docs) == 2

        invoices = await service.list_documents(session, classification="invoice")
        assert [d.document_id for d in invoices] == ["doc-2"]

        one = await service.get_document(session, "doc-1")
        assert one is not None and one.filename == "doc-1.pdf"

        missing = await service.get_document(session, "nope")
        assert missing is None


# --------------------------------------------------------------------------- #
# Search
# --------------------------------------------------------------------------- #
async def test_search_returns_hits_with_filenames(sessionmaker):
    await _project(sessionmaker, "doc-1")
    qdrant = StubQdrant(
        [
            {"document_id": "doc-1", "chunk_index": 0, "text": "alpha", "score": 0.9},
            {"document_id": "doc-1", "chunk_index": 1, "text": "beta", "score": 0.8},
        ]
    )

    async with sessionmaker() as session:
        hits = await service.search_chunks(
            session,
            embedder=FakeEmbedder(dim=8),
            qdrant=qdrant,
            collection="documents",
            question="what is alpha?",
            limit=2,
        )

    assert len(hits) == 2
    # Filename joined in from the read model, so citations are human-readable.
    assert all(h["filename"] == "doc-1.pdf" for h in hits)
    assert hits[0]["text"] == "alpha"


async def test_search_empty_question_returns_nothing(sessionmaker):
    qdrant = StubQdrant([{"document_id": "d", "chunk_index": 0, "text": "x", "score": 1.0}])
    async with sessionmaker() as session:
        hits = await service.search_chunks(
            session,
            embedder=FakeEmbedder(dim=8),
            qdrant=qdrant,
            collection="documents",
            question="   ",
        )
    assert hits == []


async def test_search_filters_by_document_id(sessionmaker):
    await _project(sessionmaker, "doc-1")
    await _project(sessionmaker, "doc-2")
    qdrant = StubQdrant(
        [
            {"document_id": "doc-1", "chunk_index": 0, "text": "a", "score": 0.9},
            {"document_id": "doc-2", "chunk_index": 0, "text": "b", "score": 0.8},
        ]
    )

    async with sessionmaker() as session:
        hits = await service.search_chunks(
            session,
            embedder=FakeEmbedder(dim=8),
            qdrant=qdrant,
            collection="documents",
            question="anything",
            limit=5,
            document_id="doc-2",
        )

    assert len(hits) == 1
    assert hits[0]["document_id"] == "doc-2"


# --------------------------------------------------------------------------- #
# Grounded answers (full RAG)
# --------------------------------------------------------------------------- #
async def test_answer_question_returns_answer_and_sources(sessionmaker):
    await _project(sessionmaker, "doc-1")
    qdrant = StubQdrant(
        [{"document_id": "doc-1", "chunk_index": 0, "text": "deposit is $2000", "score": 0.95}]
    )

    async with sessionmaker() as session:
        answer, sources = await service.answer_question(
            session,
            embedder=FakeEmbedder(dim=8),
            qdrant=qdrant,
            generator=FakeGenerator(),
            collection="documents",
            question="what is the deposit?",
            limit=3,
        )

    assert "1 excerpt" in answer
    assert len(sources) == 1
    assert sources[0]["text"] == "deposit is $2000"


async def test_answer_with_no_hits_says_so(sessionmaker):
    qdrant = StubQdrant([])
    async with sessionmaker() as session:
        answer, sources = await service.answer_question(
            session,
            embedder=FakeEmbedder(dim=8),
            qdrant=qdrant,
            generator=FakeGenerator(),
            collection="documents",
            question="anything?",
        )
    assert sources == []
    assert "do not contain enough information" in answer


# --------------------------------------------------------------------------- #
# Prompt construction
# --------------------------------------------------------------------------- #
def test_build_user_message_includes_citable_labels():
    msg = build_user_message(
        "q?",
        [{"document_id": "d1", "filename": "a.pdf", "chunk_index": 2, "text": "body", "score": 0.5}],
    )
    assert "a.pdf" in msg and "chunk 2" in msg and "body" in msg and "q?" in msg


def test_build_user_message_handles_no_contexts():
    msg = build_user_message("q?", [])
    assert "No relevant document excerpts" in msg
