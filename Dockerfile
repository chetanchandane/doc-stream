# syntax=docker/dockerfile:1.7
#
# DocStream images — one Dockerfile, one image per service.
#
# All five services share a Python package and a dependency set, so they share a
# build cache and a runtime layer, then diverge only in their entrypoint. Build a
# single service with --target:
#
#   docker build --target gateway -t docstream/gateway:dev .
#   docker build --target query   -t docstream/query:dev   .
#
# Targets: gateway · extraction · enrichment · projector · query
#          migrate (one-shot: alembic upgrade head)
#          topics  (one-shot: create Kafka topics)
#
# NOTE ON IMAGE SIZE: every service installs the full dependency set, so the
# images are near-identical in size. Separate images buy independent deploys,
# rollbacks, and scaling — not smaller layers. To actually slim them, split
# dependencies into per-service extras in pyproject.toml and pass --extra here.

ARG PYTHON_VERSION=3.11

# --------------------------------------------------------------------------- #
# Builder: resolve and install dependencies into a self-contained venv.
# --------------------------------------------------------------------------- #
FROM python:${PYTHON_VERSION}-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Dependency layer first: cached until pyproject.toml or uv.lock actually change,
# so source edits don't trigger a full reinstall.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Project layer. README.md is required because hatchling reads it as the package
# readme; the build fails without it.
COPY README.md ./
COPY src/ ./src/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# --------------------------------------------------------------------------- #
# Runtime: shared base for every service. No uv, no build tooling, non-root.
# --------------------------------------------------------------------------- #
FROM python:${PYTHON_VERSION}-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

RUN groupadd --system docstream \
 && useradd --system --gid docstream --create-home docstream

WORKDIR /app

COPY --from=builder --chown=docstream:docstream /app/.venv /app/.venv
COPY --chown=docstream:docstream src/ /app/src/
COPY --chown=docstream:docstream alembic/ /app/alembic/
COPY --chown=docstream:docstream alembic.ini /app/alembic.ini
COPY --chown=docstream:docstream scripts/ /app/scripts/

# Uploaded bytes land here. The gateway writes them and the extraction worker
# reads them, so in compose/k8s this path MUST be a shared volume.
RUN mkdir -p /app/.data/uploads && chown -R docstream:docstream /app/.data

USER docstream

# --------------------------------------------------------------------------- #
# One-shot jobs
# --------------------------------------------------------------------------- #
FROM runtime AS migrate
CMD ["alembic", "upgrade", "head"]

FROM runtime AS topics
CMD ["python", "scripts/create_topics.py"]

# --------------------------------------------------------------------------- #
# Command side (write path)
# --------------------------------------------------------------------------- #
FROM runtime AS gateway
EXPOSE 8000
# curl isn't in the slim image, so probe with the stdlib.
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=5 \
    CMD ["python", "-c", "import urllib.request;urllib.request.urlopen('http://localhost:8000/healthz',timeout=3)"]
CMD ["uvicorn", "docstream.gateway.app:app", "--host", "0.0.0.0", "--port", "8000"]

FROM runtime AS extraction
CMD ["python", "-m", "docstream.extraction.worker"]

FROM runtime AS enrichment
CMD ["python", "-m", "docstream.enrichment.worker"]

# --------------------------------------------------------------------------- #
# Query side (read path)
# --------------------------------------------------------------------------- #
FROM runtime AS projector
CMD ["python", "-m", "docstream.projection.worker"]

FROM runtime AS query
EXPOSE 8001
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=5 \
    CMD ["python", "-c", "import urllib.request;urllib.request.urlopen('http://localhost:8001/healthz',timeout=3)"]
CMD ["uvicorn", "docstream.query.app:app", "--host", "0.0.0.0", "--port", "8001"]
