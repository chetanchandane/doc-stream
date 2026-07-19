"""Local filesystem storage.

Fine for tests and single-process local dev. NOT suitable once services run in
separate containers or pods: each gets its own filesystem, so bytes written by
the gateway are invisible to the extraction worker unless a shared volume is
mounted. Use the S3 backend for anything multi-node — see ``storage/s3.py``.

File I/O runs in a worker thread so this satisfies the async ``Storage``
contract without blocking the event loop on a slow disk or a large upload.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from urllib.parse import unquote, urlparse


class LocalStorage:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    # --- async interface (what application code uses) ----------------------- #
    async def save(self, document_id: str, filename: str, data: bytes) -> str:
        return await asyncio.to_thread(self.save_sync, document_id, filename, data)

    async def read(self, uri: str) -> bytes:
        return await asyncio.to_thread(self.read_sync, uri)

    # --- sync implementation (handy in scripts and fixtures) ---------------- #
    def save_sync(self, document_id: str, filename: str, data: bytes) -> str:
        """Persist bytes under ``<root>/<document_id>/<filename>``; return a URI."""
        safe_name = Path(filename).name or "document.bin"
        dest_dir = self.root / document_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / safe_name
        dest.write_bytes(data)
        return dest.resolve().as_uri()

    def read_sync(self, uri: str) -> bytes:
        return self._path_for(uri).read_bytes()

    @staticmethod
    def _path_for(uri: str) -> Path:
        """Resolve a file:// URI (or bare path) to a local Path."""
        if uri.startswith("file://"):
            return Path(unquote(urlparse(uri).path))
        return Path(uri)
