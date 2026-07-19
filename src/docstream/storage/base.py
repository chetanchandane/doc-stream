"""The storage interface every backend implements.

Deliberately object-store shaped: ``save`` returns an opaque URI, and ``read``
takes one back. Callers never build paths, so the same code works against local
disk in tests and S3/MinIO in a cluster. The URI is what travels on the events,
so it must be resolvable by whichever service picks the event up.

The methods are **async** because the real backend is network I/O; making them
sync would block the event loop of every service that touches a document.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class Storage(Protocol):
    """Raw-bytes storage for uploaded and derived documents."""

    async def save(self, document_id: str, filename: str, data: bytes) -> str:
        """Persist ``data`` and return a URI that ``read`` can resolve."""
        ...

    async def read(self, uri: str) -> bytes:
        """Fetch the bytes previously stored at ``uri``."""
        ...
