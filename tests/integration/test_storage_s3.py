"""S3Storage against a REAL MinIO container.

The unit tests cover URI parsing and key layout; only a real S3 server proves
the client calls, bucket bootstrap, and byte round-trip actually work — the same
reason the Qdrant contract tests exist.

This is the backend that makes the services stateless, so it's worth verifying
for real before the Helm chart depends on it.
"""

from __future__ import annotations

import uuid

import pytest

from docstream.storage.s3 import S3Storage

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
def s3(minio_endpoint) -> S3Storage:
    storage = S3Storage(
        f"test-{uuid.uuid4().hex[:8]}",
        endpoint_url=minio_endpoint,
        access_key="docstream",
        secret_key="docstream123",
    )
    storage.ensure_bucket()
    return storage


async def test_round_trip(s3):
    uri = await s3.save("doc-1", "lease.txt", b"the deposit is $2000")
    assert uri == f"s3://{s3.bucket}/doc-1/lease.txt"
    assert await s3.read(uri) == b"the deposit is $2000"


async def test_binary_payload_survives(s3):
    """Documents are arbitrary bytes (PDFs), not text."""
    blob = bytes(range(256)) * 32
    uri = await s3.save("doc-2", "scan.pdf", blob)
    assert await s3.read(uri) == blob


async def test_documents_are_isolated_by_id(s3):
    a = await s3.save("doc-a", "same.txt", b"first")
    b = await s3.save("doc-b", "same.txt", b"second")
    assert a != b
    assert await s3.read(a) == b"first"
    assert await s3.read(b) == b"second"


async def test_resave_overwrites(s3):
    await s3.save("doc-3", "f.txt", b"v1")
    uri = await s3.save("doc-3", "f.txt", b"v2")
    assert await s3.read(uri) == b"v2"


async def test_ensure_bucket_is_idempotent(s3):
    # Workers call this on startup; a second call must not raise.
    s3.ensure_bucket()
    s3.ensure_bucket()
    uri = await s3.save("doc-4", "f.txt", b"ok")
    assert await s3.read(uri) == b"ok"


async def test_reading_a_missing_key_raises(s3):
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError):  # NoSuchKey
        await s3.read(f"s3://{s3.bucket}/nope/missing.txt")


async def test_uri_is_portable_across_client_instances(minio_endpoint, s3):
    """The whole point: another SERVICE, with its own client, resolves the URI.

    This is what a shared filesystem could not give us across pods.
    """
    uri = await s3.save("doc-5", "shared.txt", b"written by the gateway")

    other_service = S3Storage(
        s3.bucket,
        endpoint_url=minio_endpoint,
        access_key="docstream",
        secret_key="docstream123",
    )
    assert await other_service.read(uri) == b"written by the gateway"
