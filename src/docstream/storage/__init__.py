"""Raw-bytes storage for uploaded and derived documents.

Backend is chosen by config (``DOCSTREAM_STORAGE__BACKEND``):

* ``local`` — filesystem; fine for tests and single-process dev.
* ``s3``    — any S3-compatible store (MinIO locally, S3 in a cluster). Required
              once services run as separate pods, since they no longer share a
              filesystem.

Application code depends on the :class:`Storage` protocol, never on a concrete
backend, so switching is a config change rather than a code change.
"""

from functools import lru_cache

from docstream.common.config import get_settings
from docstream.storage.base import Storage
from docstream.storage.local import LocalStorage
from docstream.storage.s3 import S3Storage

__all__ = ["Storage", "LocalStorage", "S3Storage", "get_storage"]


@lru_cache
def get_storage() -> Storage:
    """Build the configured storage backend (cached per process)."""
    settings = get_settings().storage

    if settings.backend == "s3":
        storage = S3Storage(
            settings.bucket,
            endpoint_url=settings.endpoint_url,
            access_key=settings.access_key,
            secret_key=settings.secret_key,
            region=settings.region,
        )
        # Cheap and idempotent: a fresh MinIO/dev environment works with no
        # manual bucket setup.
        storage.ensure_bucket()
        return storage

    if settings.backend != "local":
        raise ValueError(
            f"unknown storage backend {settings.backend!r} (expected 'local' or 's3')"
        )

    return LocalStorage(settings.dir)
