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
    """Where raw uploaded bytes and extracted text live.

    ``local`` is fine in tests and single-process dev. Once services run as
    separate containers/pods they no longer share a filesystem, so anything
    beyond a single node needs ``s3`` (MinIO locally, S3 in a cluster).
    """

    backend: str = "local"  # "local" | "s3"

    # --- local backend ---
    dir: str = ".data/uploads"

    # --- s3 backend (MinIO or AWS) ---
    bucket: str = "docstream"
    # Point at MinIO locally (http://minio:9000); leave unset for real AWS.
    endpoint_url: str | None = None
    access_key: str = ""
    secret_key: str = ""
    region: str = "us-east-1"


class QuerySettings(BaseSettings):
    """Read-side retrieval tuning.

    Vector search always returns the top-K nearest neighbours, however poor the
    match — so with a small corpus a specific question happily returns unrelated
    chunks. These two cutoffs drop weak hits so the cited sources reflect what
    actually informed the answer.
    """

    # Absolute floor: cosine scores below this are never relevant. Set 0 to disable.
    min_score: float = 0.2
    # Relative floor: drop hits scoring below this fraction of the BEST hit.
    # Adapts to queries where everything scores high or everything scores low.
    # Set 0 to disable.
    relative_cutoff: float = 0.5
    # Default number of chunks fed to the LLM.
    default_limit: int = 5


class ConsumerSettings(BaseSettings):
    """Retry + dead-letter policy shared by the workers.

    A handler failure is retried by re-publishing the event to its own topic with
    an incremented ``attempt``; after ``max_attempts`` total tries the event is
    routed to ``<topic>.DLQ`` and the job is marked failed.
    """

    max_attempts: int = 5  # total processing attempts before the DLQ
    backoff_seconds: float = 1.0  # linear backoff base between retries


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
    consumer: ConsumerSettings = Field(default_factory=ConsumerSettings)
    query: QuerySettings = Field(default_factory=QuerySettings)


@lru_cache
def get_settings() -> Settings:
    """Cached singleton so config is parsed once per process."""
    return Settings()
