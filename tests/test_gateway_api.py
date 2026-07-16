"""End-to-end HTTP tests for the API Gateway (relay disabled in-process)."""

from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from docstream.common.config import get_settings


@pytest_asyncio.fixture
async def client(sessionmaker, tmp_path, monkeypatch):
    # Point storage at a temp dir and keep the relay out of the app lifespan.
    monkeypatch.setenv("DOCSTREAM_STORAGE__DIR", str(tmp_path / "uploads"))
    monkeypatch.setenv("DOCSTREAM_RELAY__RUN_IN_PROCESS", "false")
    get_settings.cache_clear()
    # storage helper is also cached
    from docstream.storage import local as storage_local

    storage_local.get_storage.cache_clear()

    from docstream.gateway.app import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        # Trigger lifespan (startup/shutdown) around the test.
        async with app.router.lifespan_context(app):
            yield c
    get_settings.cache_clear()


async def test_healthz(client):
    resp = await client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_ingest_then_fetch_job(client):
    files = {"file": ("lease.pdf", b"%PDF-1.4 fake bytes", "application/pdf")}
    resp = await client.post("/documents", files=files)
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "pending"
    job_id = body["job_id"]

    got = await client.get(f"/jobs/{job_id}")
    assert got.status_code == 200
    job = got.json()
    assert job["filename"] == "lease.pdf"
    assert job["size_bytes"] == len(b"%PDF-1.4 fake bytes")
    assert job["content_type"] == "application/pdf"


async def test_ingest_rejects_empty_file(client):
    files = {"file": ("empty.txt", b"", "text/plain")}
    resp = await client.post("/documents", files=files)
    assert resp.status_code == 400


async def test_get_unknown_job_404(client):
    resp = await client.get("/jobs/does-not-exist")
    assert resp.status_code == 404
