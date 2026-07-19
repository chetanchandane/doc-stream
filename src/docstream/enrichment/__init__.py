"""
docstream.enrichment — Phase 1 adapters (chunking, embedding, Qdrant, LLM).

Ported from ClinRAG (https://github.com/chetanchandane/rag) and adapted to
DocStream conventions:
  - plain extracted text instead of page dicts
  - deterministic Qdrant point ids for idempotent re-upsert
  - provider logic hidden behind Embedder / LLM protocols so tests use fakes

The enrichment worker (enrichment/worker.py, Phase 2) wires the real
implementations together; unit tests inject the Fake* variants.
"""

from docstream.enrichment.chunking import chunk_text
from docstream.enrichment.embedding import Embedder, FakeEmbedder, OpenAIEmbedder
from docstream.enrichment.llm import LLM, AnthropicLLM, EnrichmentResult, FakeLLM
from docstream.enrichment.qdrant_store import (
    ensure_collection,
    point_id,
    search,
    upsert_chunks,
)

__all__ = [
    "chunk_text",
    "Embedder",
    "OpenAIEmbedder",
    "FakeEmbedder",
    "LLM",
    "EnrichmentResult",
    "AnthropicLLM",
    "FakeLLM",
    "ensure_collection",
    "upsert_chunks",
    "search",
    "point_id",
]