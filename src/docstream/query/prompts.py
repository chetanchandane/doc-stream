"""Prompts for grounded answer generation.

Adapted from ClinRAG's src/generation/prompts.py. The rules matter more than the
wording: answer ONLY from retrieved context, say so when the context is
insufficient, and cite sources — that's what keeps a RAG answer trustworthy
instead of a confident hallucination.

All prompt text lives here; no other module in the read path contains prompts.
"""

from __future__ import annotations

SYSTEM_PROMPT = """You are a document question-answering assistant. You answer questions \
using only the excerpts retrieved from the user's own documents.

RULES - follow these exactly:
1. Answer using ONLY the provided document excerpts. Never use outside knowledge.
2. If the excerpts do not contain enough information to answer, say exactly:
   "The available documents do not contain enough information to answer that."
3. Cite the source for every factual claim, like this: [source: <filename or document_id>, chunk <n>]
4. Be concise and precise. Do not speculate or pad the answer."""


def build_user_message(question: str, contexts: list[dict]) -> str:
    """Format retrieved chunks into a grounded user message.

    Each chunk is labelled with its document and chunk index so the model can
    cite accurately, and so a human reading the trace can see exactly what the
    model was given.
    """
    if not contexts:
        return (
            "No relevant document excerpts were retrieved for this query.\n\n"
            f"Question: {question}"
        )

    context_block = "\n\n---\n\n".join(
        f"[source: {c.get('filename') or c.get('document_id')}, "
        f"chunk {c.get('chunk_index')} | score: {c.get('score'):.3f}]\n{c.get('text')}"
        if isinstance(c.get("score"), (int, float))
        else f"[source: {c.get('filename') or c.get('document_id')}, "
        f"chunk {c.get('chunk_index')}]\n{c.get('text')}"
        for c in contexts
    )

    return (
        "Use the following document excerpts to answer the question.\n\n"
        f"=== DOCUMENT EXCERPTS ===\n{context_block}\n\n"
        f"=== QUESTION ===\n{question}\n\n"
        "=== ANSWER ==="
    )
