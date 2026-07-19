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
from docstream.query.retrieval import filter_by_relevance


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
# Relevance filtering
# --------------------------------------------------------------------------- #
# Scores below are the REAL ones observed asking "what is the security deposit?"
# against a corpus holding one lease and one resume: the lease chunk scored 0.42,
# the four resume chunks 0.077 / 0.068 / 0.066 / 0.054. Before filtering, all
# five were returned as "sources" for the answer.
_OBSERVED = [
    {"document_id": "lease", "chunk_index": 0, "text": "deposit $2000", "score": 0.4199896},
    {"document_id": "resume", "chunk_index": 5, "text": "Vodafone", "score": 0.07742128},
    {"document_id": "resume", "chunk_index": 6, "text": "CI/CD", "score": 0.06768199},
    {"document_id": "resume", "chunk_index": 0, "text": "Chetan", "score": 0.06571977},
    {"document_id": "resume", "chunk_index": 8, "text": "TriageAI", "score": 0.054184772},
]


def test_filter_drops_irrelevant_sources_from_real_scores():
    kept = filter_by_relevance(_OBSERVED, min_score=0.2, relative_cutoff=0.5)
    assert len(kept) == 1
    assert kept[0]["document_id"] == "lease"


def test_filter_absolute_floor_only():
    kept = filter_by_relevance(_OBSERVED, min_score=0.06, relative_cutoff=0.0)
    # 0.054 drops; the rest survive the floor.
    assert [round(h["score"], 3) for h in kept] == [0.420, 0.077, 0.068, 0.066]


def test_filter_relative_cutoff_adapts_to_the_top_hit():
    """When everything scores low but comparably, relative keeps the cluster."""
    hits = [
        {"score": 0.34, "document_id": "a"},
        {"score": 0.30, "document_id": "b"},
        {"score": 0.05, "document_id": "c"},
    ]
    kept = filter_by_relevance(hits, min_score=0.0, relative_cutoff=0.5)
    assert [h["document_id"] for h in kept] == ["a", "b"]


def test_filter_returns_empty_when_nothing_is_relevant():
    weak = [{"score": 0.05, "document_id": "x"}, {"score": 0.04, "document_id": "y"}]
    assert filter_by_relevance(weak, min_score=0.2, relative_cutoff=0.5) == []


def test_filter_disabled_by_zeros():
    assert filter_by_relevance(_OBSERVED, min_score=0.0, relative_cutoff=0.0) == _OBSERVED


def test_filter_handles_empty_input():
    assert filter_by_relevance([], min_score=0.2, relative_cutoff=0.5) == []


async def test_search_applies_relevance_cutoff(sessionmaker):
    """End-to-end through the service: noise is dropped before it reaches sources."""
    await _project(sessionmaker, "lease")
    qdrant = StubQdrant(
        [
            {"document_id": "lease", "chunk_index": 0, "text": "deposit $2000", "score": 0.42},
            {"document_id": "lease", "chunk_index": 1, "text": "unrelated", "score": 0.06},
        ]
    )
    async with sessionmaker() as session:
        hits = await service.search_chunks(
            session,
            embedder=FakeEmbedder(dim=8),
            qdrant=qdrant,
            collection="documents",
            question="security deposit",
            limit=5,
            min_score=0.2,
            relative_cutoff=0.5,
        )
    assert len(hits) == 1
    assert hits[0]["chunk_index"] == 0


async def test_answer_says_it_does_not_know_when_all_hits_are_weak(sessionmaker):
    """Filtering to zero is better than citing noise: the model can decline."""
    qdrant = StubQdrant(
        [{"document_id": "resume", "chunk_index": 0, "text": "unrelated", "score": 0.05}]
    )
    async with sessionmaker() as session:
        answer, sources = await service.answer_question(
            session,
            embedder=FakeEmbedder(dim=8),
            qdrant=qdrant,
            generator=FakeGenerator(),
            collection="documents",
            question="what is the security deposit?",
            min_score=0.2,
            relative_cutoff=0.5,
        )
    assert sources == []
    assert "do not contain enough information" in answer


# --------------------------------------------------------------------------- #
# Prompt construction
# --------------------------------------------------------------------------- #
def test_build_user_message_includes_citable_labels():
    msg = build_user_message(
        "q?",
        [{
            "document_id": "d1", "filename": "a.pdf",
            "chunk_index": 2, "text": "body", "score": 0.5,
        }],
    )
    assert "a.pdf" in msg and "chunk 2" in msg and "body" in msg and "q?" in msg


def test_build_user_message_handles_no_contexts():
    msg = build_user_message("q?", [])
    assert "No relevant document excerpts" in msg
