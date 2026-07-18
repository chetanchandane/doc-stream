"""FastAPI query service — the read side of CQRS.

Endpoints:
    GET  /healthz                 liveness
    GET  /documents               list the read model
    GET  /documents/{document_id} one read-model row
    GET  /search?q=...            semantic search (excerpts, no LLM)
    POST /ask                     grounded RAG answer + sources

Runs as its own process/deployment so read traffic scales independently of the
ingest pipeline. It reads ONLY the document_view read model and Qdrant — it
never writes, and never touches the jobs write model.

Run it standalone (note the port, so it can sit alongside the gateway):

    uvicorn docstream.query.app:app --port 8001
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from functools import lru_cache

from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from docstream.common.config import get_settings
from docstream.db.base import get_session
from docstream.query import service
from docstream.query.schemas import (
    AskRequest,
    AskResponse,
    Chunk,
    DocumentSummary,
    SearchResponse,
)


@lru_cache
def _dependencies():
    """Build the embedder, generator, and Qdrant client once per process.

    Imported lazily inside the function so tests can override the dependency
    without needing provider SDKs or a live Qdrant.
    """
    import anthropic
    from openai import AsyncOpenAI
    from qdrant_client import AsyncQdrantClient

    from docstream.enrichment.embedding import OpenAIEmbedder
    from docstream.query.generation import AnthropicGenerator

    settings = get_settings()

    embedder = OpenAIEmbedder(
        AsyncOpenAI(
            api_key=settings.embedding.api_key,
            base_url=settings.embedding.base_url or None,
        ),
        model=settings.embedding.model,
        dim=settings.embedding.dim,
    )
    generator = AnthropicGenerator(
        anthropic.AsyncAnthropic(
            api_key=settings.llm.api_key, base_url=settings.llm.base_url or None
        ),
        model=settings.llm.model,
        max_tokens=settings.llm.max_tokens,
    )
    qdrant = AsyncQdrantClient(url=settings.qdrant.url, api_key=settings.qdrant.api_key)
    return embedder, generator, qdrant


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    yield


app = FastAPI(title="DocStream Query API", version="0.1.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/documents", response_model=list[DocumentSummary])
async def list_documents(
    limit: int = Query(50, ge=1, le=200),
    classification: str | None = None,
    session: AsyncSession = Depends(get_session),
) -> list[DocumentSummary]:
    views = await service.list_documents(
        session, limit=limit, classification=classification
    )
    return [DocumentSummary.from_view(v) for v in views]


@app.get("/documents/{document_id}", response_model=DocumentSummary)
async def get_document(
    document_id: str,
    session: AsyncSession = Depends(get_session),
) -> DocumentSummary:
    view = await service.get_document(session, document_id)
    if view is None:
        # Either the document doesn't exist, or it isn't enriched yet — the read
        # model is eventually consistent with the pipeline.
        raise HTTPException(status_code=404, detail="Document not found or not yet indexed.")
    return DocumentSummary.from_view(view)


@app.get("/search", response_model=SearchResponse)
async def search(
    q: str = Query(..., min_length=1, description="Natural-language query"),
    limit: int = Query(5, ge=1, le=50),
    document_id: str | None = None,
    session: AsyncSession = Depends(get_session),
) -> SearchResponse:
    embedder, _generator, qdrant = _dependencies()
    hits = await service.search_chunks(
        session,
        embedder=embedder,
        qdrant=qdrant,
        collection=get_settings().qdrant.collection,
        question=q,
        limit=limit,
        document_id=document_id,
    )
    return SearchResponse(query=q, count=len(hits), results=[Chunk(**h) for h in hits])


@app.post("/ask", response_model=AskResponse)
async def ask(
    request: AskRequest,
    session: AsyncSession = Depends(get_session),
) -> AskResponse:
    embedder, generator, qdrant = _dependencies()
    answer, sources = await service.answer_question(
        session,
        embedder=embedder,
        qdrant=qdrant,
        generator=generator,
        collection=get_settings().qdrant.collection,
        question=request.question,
        limit=request.limit,
        document_id=request.document_id,
    )
    return AskResponse(
        question=request.question,
        answer=answer,
        sources=[Chunk(**s) for s in sources],
    )
