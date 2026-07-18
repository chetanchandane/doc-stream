"""
chunking.py — split extracted document text into overlapping chunks.

Ported from ClinRAG src/ingestion/splitter.py (chunk_pages). Differences:
  - operates on a single plain-text string (DocStream stores extracted text via
    storage.read(text_uri)), not on page-level {"text","page","source"} dicts.
  - returns list[str] instead of list[dict]; the worker attaches document_id /
    chunk index when it upserts to Qdrant.

GOTCHA carried over from ClinRAG: RecursiveCharacterTextSplitter counts
CHARACTERS, not tokens, despite ClinRAG's config comment. So chunk_size=512
means ~512 characters. Retune if you want token-based sizing.
"""

from __future__ import annotations

from langchain_text_splitters import RecursiveCharacterTextSplitter

# Same separator ladder ClinRAG uses: paragraph -> line -> sentence -> word.
_SEPARATORS = ["\n\n", "\n", ". ", " "]


def chunk_text(text: str, chunk_size: int = 512, chunk_overlap: int = 64) -> list[str]:
    """
    Split `text` into overlapping chunks.

    Args:
        text:          the full extracted document text.
        chunk_size:    max characters per chunk (see GOTCHA above).
        chunk_overlap: characters shared between consecutive chunks.

    Returns:
        A list of non-empty, stripped chunk strings. Empty input -> [].
    """
    if not text or not text.strip():
        return []

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        separators=_SEPARATORS,
    )

    return [chunk.strip() for chunk in splitter.split_text(text) if chunk.strip()]
