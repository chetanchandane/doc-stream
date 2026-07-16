"""Pure text extraction. No I/O, no DB — easy to unit test.

Week 1 keeps this deliberately thin: plain-text decode and PDF text via pypdf,
with a best-effort fallback. OCR for scanned/image PDFs is a Week 4 stretch.
"""

from __future__ import annotations

import io
from dataclasses import dataclass


@dataclass(frozen=True)
class Extraction:
    text: str
    page_count: int
    method: str  # "native", "pdf", or "fallback"


def extract_text(filename: str, content_type: str, data: bytes) -> Extraction:
    """Extract text from raw bytes based on content type / extension."""
    name = filename.lower()
    ctype = (content_type or "").lower()

    if ctype == "application/pdf" or name.endswith(".pdf"):
        return _extract_pdf(data)

    if ctype.startswith("text/") or name.endswith((".txt", ".md", ".csv", ".json")):
        return Extraction(
            text=data.decode("utf-8", errors="replace"),
            page_count=1,
            method="native",
        )

    # Unknown type: best-effort decode so the pipeline still moves.
    return Extraction(
        text=data.decode("utf-8", errors="replace"),
        page_count=1,
        method="fallback",
    )


def _extract_pdf(data: bytes) -> Extraction:
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(data))
    pages = [(page.extract_text() or "") for page in reader.pages]
    return Extraction(
        text="\n\n".join(pages).strip(),
        page_count=len(pages),
        method="pdf",
    )
