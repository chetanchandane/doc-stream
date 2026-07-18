"""
embedding.py — dense embedding behind a swappable interface.

Ported from ClinRAG:
  - batching + TPM/429 handling from src/ingestion/indexer.py::_embed
  - single-text query embed from src/retrieval/search.py::Searcher.embed_query

The concrete provider is hidden behind the `Embedder` protocol so the worker
injects OpenAIEmbedder in production and tests inject FakeEmbedder. `embed`
handles both a list of chunks (index-time) and a single-element list (query-time).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import struct
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)

# ClinRAG's Tier-1 OpenAI defaults: 40k TPM for text-embedding-3-small.
_EMBED_BATCH_SIZE = 20      # texts per API call
_EMBED_BATCH_DELAY = 1.5    # seconds between batches
_RATE_LIMIT_COOLDOWN = 60   # seconds to wait after a 429 before one retry


@runtime_checkable
class Embedder(Protocol):
    """Dense embedder interface. Returns one vector per input text, in order."""

    dim: int

    async def embed(self, texts: list[str]) -> list[list[float]]:
        ...


class OpenAIEmbedder:
    """
    OpenAI dense embedder (text-embedding-3-small by default, 1536 dims).

    Construct with an already-configured AsyncOpenAI client so credentials and
    base_url live in config, not here:

        from openai import AsyncOpenAI
        embedder = OpenAIEmbedder(AsyncOpenAI(api_key=...), model=..., dim=...)
    """

    def __init__(self, client, model: str = "text-embedding-3-small", dim: int = 1536):
        self._client = client
        self.model = model
        self.dim = dim

    async def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        # Imported here so the module imports even where openai isn't installed
        # (e.g. a test env that only exercises FakeEmbedder).
        from openai import RateLimitError

        vectors: list[list[float]] = []
        for start in range(0, len(texts), _EMBED_BATCH_SIZE):
            batch = texts[start : start + _EMBED_BATCH_SIZE]
            try:
                resp = await self._client.embeddings.create(model=self.model, input=batch)
            except RateLimitError:
                logger.warning("Embedding rate limit hit; cooling down %ss", _RATE_LIMIT_COOLDOWN)
                await asyncio.sleep(_RATE_LIMIT_COOLDOWN)
                resp = await self._client.embeddings.create(model=self.model, input=batch)

            vectors.extend(item.embedding for item in resp.data)

            if start + _EMBED_BATCH_SIZE < len(texts):
                await asyncio.sleep(_EMBED_BATCH_DELAY)

        return vectors


class FakeEmbedder:
    """
    Deterministic embedder for tests — no network. Same text always yields the
    same vector, so idempotency tests can assert stable results.
    """

    def __init__(self, dim: int = 1536):
        self.dim = dim

    async def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._vector(t) for t in texts]

    def _vector(self, text: str) -> list[float]:
        # Hash-seed a repeatable pseudo-random vector in [0, 1).
        seed = hashlib.sha256(text.encode("utf-8")).digest()
        out: list[float] = []
        i = 0
        while len(out) < self.dim:
            chunk = hashlib.sha256(seed + struct.pack("<I", i)).digest()
            for j in range(0, len(chunk), 4):
                if len(out) >= self.dim:
                    break
                (val,) = struct.unpack("<I", chunk[j : j + 4])
                out.append(val / 0xFFFFFFFF)
            i += 1
        return out
