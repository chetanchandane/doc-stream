"""Query orchestration, independent of HTTP.

Keeping this transport-free means the read path is unit-testable with fakes and
could be exposed over gRPC or a CLI without change — and it's what makes the
service cheap to extract into its own deployment.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docstream.db.models import DocumentView
from docstream.enrichment.embedding import Embedder
from docstream.query.generation import Generator
from docstream.query.retrieval import retrieve

log = logging.getLogger("docstream.query.service")


async def list_documents(
    session: AsyncSession, *, limit: int = 50, classification: str | None = None
) -> Sequence[DocumentView]:
    """Read-model listing. No joins, no write-model access."""
    stmt = select(DocumentView).order_by(DocumentView.indexed_at.desc()).limit(limit)
    if classification is not None:
        stmt = stmt.where(DocumentView.classification == classification)
    result = await session.execute(stmt)
    return result.scalars().all()


async def get_document(session: AsyncSession, document_id: str) -> DocumentView | None:
    return await session.get(DocumentView, document_id)


async def _attach_filenames(
    session: AsyncSession, hits: list[dict]
) -> list[dict]:
    """Enrich raw Qdrant hits with filenames from the read model.

    Qdrant payloads carry ``document_id`` but not the filename; the read model
    has it. One batched lookup keeps citations human-readable.
    """
    doc_ids = {h.get("document_id") for h in hits if h.get("document_id")}
    if not doc_ids:
        return hits

    rows = (
        await session.execute(
            select(DocumentView).where(DocumentView.document_id.in_(doc_ids))
        )
    ).scalars().all()
    names = {row.document_id: row.filename for row in rows}

    for hit in hits:
        hit["filename"] = names.get(hit.get("document_id"))
    return hits


async def search_chunks(
    session: AsyncSession,
    *,
    embedder: Embedder,
    qdrant,
    collection: str,
    question: str,
    limit: int = 5,
    document_id: str | None = None,
) -> list[dict]:
    """Semantic search: retrieved excerpts, no generation."""
    hits = await retrieve(
        embedder=embedder,
        qdrant=qdrant,
        collection=collection,
        question=question,
        limit=limit,
        document_id=document_id,
    )
    return await _attach_filenames(session, hits)


async def answer_question(
    session: AsyncSession,
    *,
    embedder: Embedder,
    qdrant,
    generator: Generator,
    collection: str,
    question: str,
    limit: int = 5,
    document_id: str | None = None,
) -> tuple[str, list[dict]]:
    """Full RAG: retrieve, then generate a grounded answer.

    Returns ``(answer, sources)``. Sources are returned alongside the answer so
    the caller can verify the grounding rather than trusting it.
    """
    contexts = await search_chunks(
        session,
        embedder=embedder,
        qdrant=qdrant,
        collection=collection,
        question=question,
        limit=limit,
        document_id=document_id,
    )
    answer = await generator.generate(question, contexts)
    log.info("answered question with %d context chunk(s)", len(contexts))
    return answer, contexts
