"""Local filesystem storage.

Good enough for local dev and tests. The interface (``save`` returns a URI) is
deliberately object-store shaped so this can be swapped for S3/GCS later without
touching callers.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from urllib.parse import unquote, urlparse

from docstream.common.config import get_settings


class LocalStorage:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def save(self, document_id: str, filename: str, data: bytes) -> str:
        """Persist bytes under ``<root>/<document_id>/<filename>`` and return a URI."""
        safe_name = Path(filename).name or "document.bin"
        dest_dir = self.root / document_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / safe_name
        dest.write_bytes(data)
        return dest.resolve().as_uri()

    def read(self, uri: str) -> bytes:
        return self._path_for(uri).read_bytes()

    @staticmethod
    def _path_for(uri: str) -> Path:
        """Resolve a file:// URI (or bare path) to a local Path."""
        if uri.startswith("file://"):
            return Path(unquote(urlparse(uri).path))
        return Path(uri)


@lru_cache
def get_storage() -> LocalStorage:
    return LocalStorage(get_settings().storage.dir)
