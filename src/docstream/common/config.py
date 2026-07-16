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


@lru_cache
def get_settings() -> Settings:
    """Cached singleton so config is parsed once per process."""
    return Settings()
