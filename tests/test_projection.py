"""Projector: documents.enriched -> document_view read model."""

from __future__ import annotations

from sqlalchemy import func, select

from docstream.common.events import DocumentEnriched, make_event
from docstream.db.models import DocumentView
from docstream.projection.worker import handle_enriched


def _enriched(document_id: str = "doc-1", **overrides):
    payload = dict(
        job_id="job-1",
        document_id=document_id,
        filename="resume.pdf",
        classification="resume",
        summary="A summary.",
        chunk_count=10,
        vector_ids=["v1", "v2"],
    )
    payload.update(overrides)
    return make_event(DocumentEnriched(**payload), source="test")


async def test_projects_enriched_event_into_view(sessionmaker):
    envelope = _enriched()

    async with sessionmaker() as session:
        async with session.begin():
            await handle_enriched(session, envelope)

    async with sessionmaker() as session:
        view = await session.get(DocumentView, "doc-1")
        assert view is not None
        assert view.job_id == "job-1"
        assert view.filename == "resume.pdf"
        assert view.classification == "resume"
        assert view.summary == "A summary."
        assert view.chunk_count == 10
        assert view.indexed_at is not None


async def test_duplicate_event_is_skipped(sessionmaker):
    """Same event delivered twice -> still exactly one row, no double-apply."""
    envelope = _enriched()

    for _ in range(2):
        async with sessionmaker() as session:
            async with session.begin():
                await handle_enriched(session, envelope)

    async with sessionmaker() as session:
        count = (
            await session.execute(select(func.count()).select_from(DocumentView))
        ).scalar_one()
        assert count == 1


async def test_reprojection_upserts_rather_than_duplicating(sessionmaker):
    """A NEW event for the same document overwrites the row (upsert semantics)."""
    async with sessionmaker() as session:
        async with session.begin():
            await handle_enriched(session, _enriched())

    # Distinct event_id (a corrected re-run), same document_id.
    async with sessionmaker() as session:
        async with session.begin():
            await handle_enriched(
                session,
                _enriched(classification="cv", summary="Updated.", chunk_count=12),
            )

    async with sessionmaker() as session:
        count = (
            await session.execute(select(func.count()).select_from(DocumentView))
        ).scalar_one()
        assert count == 1
        view = await session.get(DocumentView, "doc-1")
        assert view.classification == "cv"
        assert view.summary == "Updated."
        assert view.chunk_count == 12


async def test_separate_documents_get_separate_rows(sessionmaker):
    async with sessionmaker() as session:
        async with session.begin():
            await handle_enriched(session, _enriched(document_id="doc-1"))
    async with sessionmaker() as session:
        async with session.begin():
            await handle_enriched(session, _enriched(document_id="doc-2"))

    async with sessionmaker() as session:
        count = (
            await session.execute(select(func.count()).select_from(DocumentView))
        ).scalar_one()
        assert count == 2
