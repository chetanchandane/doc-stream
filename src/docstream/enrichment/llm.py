"""
llm.py — LLM enrichment (classification + summary + fields) behind an interface.

Ported from ClinRAG src/generation/llm_client.py: the AsyncAnthropic wrapper and
messages.create(system=..., messages=...) call shape carry over directly.

WHAT DOES NOT CARRY OVER: the prompt. ClinRAG's prompts (src/generation/prompts.py)
are grounded citation Q&A prompts. Enrichment is a different job — classify the
document, summarize it, and pull structured fields — so we ask for JSON and parse
it into EnrichmentResult. Tune ENRICHMENT_SYSTEM_PROMPT and the classification
label set for your document domain.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@dataclass
class EnrichmentResult:
    """Structured output of one enrichment pass over a document."""

    classification: str
    summary: str
    fields: dict = field(default_factory=dict)


@runtime_checkable
class LLM(Protocol):
    """Enrichment interface: full document text in, structured result out."""

    async def enrich(self, text: str) -> EnrichmentResult:
        ...


ENRICHMENT_SYSTEM_PROMPT = """You are a document-intelligence assistant. You are given the \
full text of a document. Return a SINGLE JSON object and nothing else, with exactly these keys:

  "classification": a short document-type label (e.g. "invoice", "contract", "lease", \
"report", "email", "other").
  "summary": a 2-3 sentence plain-language summary of the document.
  "fields": an object of any salient key/value details you can extract (dates, parties, \
amounts, ids). Use {} if none are clear.

Rules:
- Output ONLY the JSON object. No preamble, no markdown fences, no trailing text.
- Base everything on the provided text; do not invent facts.
"""


def _build_user_message(text: str, max_chars: int = 12000) -> str:
    # Guard against oversized inputs blowing the context / cost. Truncate rather
    # than fail; the summary/classification survive truncation well.
    body = text if len(text) <= max_chars else text[:max_chars]
    return f"=== DOCUMENT TEXT ===\n{body}\n\n=== RETURN JSON ==="


def _parse(raw: str) -> EnrichmentResult:
    """Parse the model's JSON reply, tolerating stray markdown fences."""
    cleaned = raw.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("```", 2)[1] if "```" in cleaned[3:] else cleaned.strip("`")
        cleaned = cleaned.removeprefix("json").strip()
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        logger.warning("LLM did not return valid JSON; storing raw text as summary")
        return EnrichmentResult(classification="unknown", summary=raw.strip()[:500], fields={})
    return EnrichmentResult(
        classification=str(data.get("classification", "unknown")),
        summary=str(data.get("summary", "")),
        fields=data.get("fields") or {},
    )


class AnthropicLLM:
    """
    Claude enrichment client. Construct with a configured AsyncAnthropic client:

        import anthropic
        llm = AnthropicLLM(anthropic.AsyncAnthropic(api_key=...), model=..., max_tokens=...)
    """

    def __init__(self, client, model: str = "claude-sonnet-4-6", max_tokens: int = 1024):
        self._client = client
        self.model = model
        self.max_tokens = max_tokens

    async def enrich(self, text: str) -> EnrichmentResult:
        message = await self._client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            system=ENRICHMENT_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": _build_user_message(text)}],
        )
        return _parse(message.content[0].text)


class FakeLLM:
    """
    Deterministic enrichment for tests — no network. Returns a stable result
    derived from the input so assertions are repeatable.
    """

    def __init__(self, classification: str = "test-doc"):
        self._classification = classification

    async def enrich(self, text: str) -> EnrichmentResult:
        return EnrichmentResult(
            classification=self._classification,
            summary=f"Summary of {len(text)} characters.",
            fields={"char_count": len(text)},
        )
