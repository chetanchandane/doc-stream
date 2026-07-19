"""Storage backends: the local filesystem implementation and S3 URI handling.

The S3 backend's network behaviour is covered by an integration test against a
real MinIO container (tests/integration/test_storage_s3.py); here we cover the
pure logic that doesn't need a server.
"""

from __future__ import annotations

import pytest

from docstream.storage import LocalStorage, Storage
from docstream.storage.s3 import S3Storage


# --------------------------------------------------------------------------- #
# Local backend
# --------------------------------------------------------------------------- #
async def test_local_round_trip(tmp_path):
    storage = LocalStorage(tmp_path)
    uri = await storage.save("doc-1", "lease.txt", b"hello world")
    assert uri.startswith("file://")
    assert await storage.read(uri) == b"hello world"


async def test_local_isolates_documents_by_id(tmp_path):
    storage = LocalStorage(tmp_path)
    a = await storage.save("doc-1", "same.txt", b"first")
    b = await storage.save("doc-2", "same.txt", b"second")
    assert a != b
    assert await storage.read(a) == b"first"
    assert await storage.read(b) == b"second"


async def test_local_strips_directory_traversal(tmp_path):
    """A malicious filename must not escape the storage root."""
    storage = LocalStorage(tmp_path)
    uri = await storage.save("doc-1", "../../etc/passwd", b"nope")
    assert "etc/passwd" not in uri
    assert await storage.read(uri) == b"nope"


async def test_local_overwrites_same_key(tmp_path):
    storage = LocalStorage(tmp_path)
    await storage.save("doc-1", "f.txt", b"v1")
    uri = await storage.save("doc-1", "f.txt", b"v2")
    assert await storage.read(uri) == b"v2"


def test_local_satisfies_the_storage_protocol(tmp_path):
    assert isinstance(LocalStorage(tmp_path), Storage)


# --------------------------------------------------------------------------- #
# S3 URI parsing (no network)
# --------------------------------------------------------------------------- #
def test_parse_uri_splits_bucket_and_key():
    assert S3Storage.parse_uri("s3://docstream/doc-1/lease.txt") == (
        "docstream",
        "doc-1/lease.txt",
    )


def test_parse_uri_handles_nested_keys():
    bucket, key = S3Storage.parse_uri("s3://b/a/b/c/d.txt")
    assert bucket == "b" and key == "a/b/c/d.txt"


@pytest.mark.parametrize(
    "bad",
    [
        "file:///tmp/x",       # wrong scheme
        "s3://bucket-only",    # no key
        "s3:///key-only",      # no bucket
        "not-a-uri",
    ],
)
def test_parse_uri_rejects_malformed_input(bad):
    with pytest.raises(ValueError):
        S3Storage.parse_uri(bad)


def test_key_layout_is_document_scoped_and_sanitised():
    assert S3Storage._key_for("doc-1", "lease.txt") == "doc-1/lease.txt"
    # Traversal in the filename must not escape the document prefix.
    assert S3Storage._key_for("doc-1", "../../etc/passwd") == "doc-1/passwd"
