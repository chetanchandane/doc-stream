"""Typed settings loaded from environment variables (12-factor).

Nested settings use a ``__`` delimiter, e.g. ``DOCSTREAM_KAFKA__BOOTSTRAP_SERVERS``.
See ``.env.example`` for the full list. Load once via :func:`get_settings`.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    bootstrap_servers: str = "localhost:9092"
    client_id: str = "docstream"
    # Sensible producer defaults for the outbox relay.
    acks: str = "all"
    enable_idempotence: bool = True


class PostgresSettings(BaseSettings):
    dsn: str = "postgresql+asyncpg://docstream:docstream@localhost:5432/docstream"


class RedisSettings(BaseSettings):
    url: str = "redis://localhost:6379/0"


class QdrantSettings(BaseSettings):
    url: str = "http://localhost:6333"
    # Cloud clusters need a key; local docker instance leaves it unset.
    api_key: str | None = None
    # Collection the enrichment worker upserts chunks into.
    collection: str = "documents"
    # Must equal the embedding model's dimension (see EmbeddingSettings.dim).
    vector_size: int = 1536


class EmbeddingSettings(BaseSettings):
    """Dense embeddings for the enrichment worker (Phase 2)."""

    provider: str = "openai"
    model: str = "text-embedding-3-small"
    dim: int = 1536  # text-embedding-3-small -> 1536; keep in sync with qdrant.vector_size
    api_key: str = ""
    base_url: str | None = None  # set for Azure/OpenAI-compatible/local gateways
    # Chunking is prep for embedding, so its knobs live here.
    # NOTE: RecursiveCharacterTextSplitter counts CHARACTERS, not tokens.
    chunk_size: int = 512
    chunk_overlap: int = 64


class LLMSettings(BaseSettings):
    """LLM used for enrichment (classification + summary + field extraction)."""

    provider: str = "anthropic"
    model: str = "claude-sonnet-4-6"
    api_key: str = ""
    base_url: str | None = None
    max_tokens: int = 1024


class StorageSettings(BaseSettings):
    """Where the API Gateway persists raw uploaded bytes (local FS for now)."""

    dir: str = ".data/uploads"


class RelaySettings(BaseSettings):
    """Outbox relay: polls the outbox table and publishes to Kafka."""

    poll_interval_seconds: float = 1.0
    batch_size: int = 100
    max_attempts: int = 10
    # Run the relay as a background task inside the gateway process. Convenient
    # for local dev; in production you'd run it as its own deployment.
    run_in_process: bool = True


class Settings(BaseSettings):
    """Root settings object for every DocStream service."""

    model_config = SettingsConfigDict(
        env_prefix="DOCSTREAM_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    env: str = "local"
    log_level: str = "INFO"

    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    qdrant: QdrantSettings = Field(default_factory=QdrantSettings)
    embedding: EmbeddingSettings = Field(default_factory=EmbeddingSettings)
    llm: LLMSettings = Field(default_factory=LLMSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)
    relay: RelaySettings = Field(default_factory=RelaySettings)


@lru_cache
def get_settings() -> Settings:
    """Cached singleton so config is parsed once per process."""
    return Settings()
