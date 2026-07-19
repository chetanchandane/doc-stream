"""Integration-test fixtures: real Postgres, Qdrant, and Kafka via testcontainers.

These tests need a running Docker daemon and are EXCLUDED from the default
``pytest`` run (see the ``-m 'not integration'`` addopts in pyproject.toml).
Run them explicitly:

    make test-integration        # or: uv run pytest -m integration -v

Why they exist: the unit suite fakes Qdrant, and a fake can only assert what we
*believe* the API is. Two production bugs (invalid point ids, and calling the
removed ``client.search()``) passed the unit suite and failed on the first real
call. These tests exercise the genuine clients so that class of bug fails here
instead of in your terminal.

Containers are session-scoped — starting them is slow, so we pay it once.
"""

from __future__ import annotations

import asyncio
import os
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

# Repo root = two levels up from tests/integration/
REPO_ROOT = Path(__file__).resolve().parents[2]

# Skip the whole package cleanly if testcontainers isn't installed.
testcontainers = pytest.importorskip("testcontainers", reason="testcontainers not installed")

from testcontainers.core.container import DockerContainer  # noqa: E402
from testcontainers.postgres import PostgresContainer  # noqa: E402


def _wait_for_http(url: str, timeout: float = 60.0) -> None:
    """Poll an HTTP endpoint until it answers (container readiness)."""
    deadline = time.time() + timeout
    last: Exception | None = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2):
                return
        except (TimeoutError, urllib.error.URLError, OSError) as exc:  # noqa: PERF203
            last = exc
            time.sleep(0.5)
    raise RuntimeError(f"Timed out waiting for {url}: {last!r}")


# --------------------------------------------------------------------------- #
# Postgres
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="session")
def postgres_dsn() -> Iterator[str]:
    """A throwaway Postgres, yielded as an asyncpg SQLAlchemy DSN."""
    with PostgresContainer("postgres:16-alpine") as pg:
        host = pg.get_container_host_ip()
        port = pg.get_exposed_port(5432)
        user = getattr(pg, "username", None) or pg.POSTGRES_USER
        password = getattr(pg, "password", None) or pg.POSTGRES_PASSWORD
        db = getattr(pg, "dbname", None) or pg.POSTGRES_DB
        yield f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}"


def _run_alembic_upgrade_sync(dsn: str, revision: str = "head") -> None:
    """Run alembic against ``dsn``. BLOCKING — see the async wrapper below.

    ``alembic/env.py`` reads the URL from ``get_settings()``, so we set the env
    var and clear the settings cache rather than fighting the config object.

    Imported inside the function on purpose: the repo has a local ``alembic/``
    directory, so importing at module scope risks shadowing the installed
    package depending on sys.path ordering.
    """
    from alembic import command
    from alembic.config import Config

    from docstream.common.config import get_settings

    previous = os.environ.get("DOCSTREAM_POSTGRES__DSN")
    os.environ["DOCSTREAM_POSTGRES__DSN"] = dsn
    get_settings.cache_clear()
    try:
        cfg = Config(str(REPO_ROOT / "alembic.ini"))
        cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
        cfg.set_main_option("sqlalchemy.url", dsn)
        command.upgrade(cfg, revision)
    finally:
        if previous is None:
            os.environ.pop("DOCSTREAM_POSTGRES__DSN", None)
        else:
            os.environ["DOCSTREAM_POSTGRES__DSN"] = previous
        get_settings.cache_clear()


async def _run_alembic_upgrade(dsn: str, revision: str = "head") -> None:
    """Async wrapper: run the migration in a worker thread.

    ``alembic/env.py`` ends with ``asyncio.run(run_migrations_online())``, which
    raises "asyncio.run() cannot be called from a running event loop" if invoked
    directly from an async test. Handing it to a thread gives it a fresh loop of
    its own.
    """
    await asyncio.to_thread(_run_alembic_upgrade_sync, dsn, revision)


@pytest.fixture(scope="session")
def alembic_upgrade() -> Callable[..., object]:
    """Expose the migration runner (awaitable) without cross-module imports."""
    return _run_alembic_upgrade


@pytest.fixture
async def migrated_sessionmaker(postgres_dsn: str, alembic_upgrade):
    """Sessionmaker on a real, fully migrated Postgres."""
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    await alembic_upgrade(postgres_dsn, "head")
    engine = create_async_engine(postgres_dsn)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


# --------------------------------------------------------------------------- #
# Qdrant (generic container — avoids depending on a testcontainers extra)
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="session")
def qdrant_url() -> Iterator[str]:
    container = DockerContainer("qdrant/qdrant:latest").with_exposed_ports(6333)
    container.start()
    try:
        url = (
            f"http://{container.get_container_host_ip()}:"
            f"{container.get_exposed_port(6333)}"
        )
        # Qdrant answers /readyz once it can serve requests.
        _wait_for_http(f"{url}/readyz")
        yield url
    finally:
        container.stop()


@pytest.fixture
async def qdrant_client(qdrant_url: str):
    """A real AsyncQdrantClient pointed at the container."""
    from qdrant_client import AsyncQdrantClient

    client = AsyncQdrantClient(url=qdrant_url)
    try:
        yield client
    finally:
        await client.close()


# --------------------------------------------------------------------------- #
# MinIO (S3-compatible object storage)
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="session")
def minio_endpoint() -> Iterator[str]:
    """A throwaway MinIO, yielded as an S3 endpoint URL."""
    container = (
        DockerContainer("minio/minio:latest")
        .with_command("server /data")
        .with_env("MINIO_ROOT_USER", "docstream")
        .with_env("MINIO_ROOT_PASSWORD", "docstream123")
        .with_exposed_ports(9000)
    )
    container.start()
    try:
        url = (
            f"http://{container.get_container_host_ip()}:"
            f"{container.get_exposed_port(9000)}"
        )
        # MinIO serves /minio/health/live once it's ready to accept S3 calls.
        _wait_for_http(f"{url}/minio/health/live")
        yield url
    finally:
        container.stop()


# --------------------------------------------------------------------------- #
# Kafka (optional; only used by the messaging contract test)
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="session")
def kafka_bootstrap() -> Iterator[str]:
    """A real broker.

    testcontainers' KafkaContainer waits for a ``[KafkaServer id=N] started``
    log line, which recent images don't always emit in that form (KRaft mode
    logs differently), so startup can time out even though the broker is fine.
    We pin an image known to match the wait strategy, and skip rather than fail
    if the broker still won't come up — a broker that won't boot is an
    environment problem, not a defect in our code, and shouldn't red the suite.
    """
    kafka = pytest.importorskip(
        "testcontainers.kafka", reason="testcontainers[kafka] not installed"
    )
    try:
        with kafka.KafkaContainer(image="confluentinc/cp-kafka:7.4.0") as broker:
            yield broker.get_bootstrap_server()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Kafka container failed to start ({type(exc).__name__}): {exc}")
