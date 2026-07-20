"""Answer generation behind a swappable interface.

Mirrors the enrichment side's pattern: a ``Generator`` protocol so the query
service depends on a capability, not a vendor, plus a ``FakeGenerator`` so the
read path is fully testable with no network.

Note this is a *different* job from enrichment's ``LLM.enrich`` (classify +
summarize a whole document). Here we answer a question from retrieved excerpts,
which needs its own prompt — hence a separate protocol rather than overloading
the enrichment client.
"""

from __future__ import annotations

import logging
from typing import Protocol, runtime_checkable

from docstream.common import metrics
from docstream.query.prompts import SYSTEM_PROMPT, build_user_message

log = logging.getLogger("docstream.query.generation")


@runtime_checkable
class Generator(Protocol):
    """Produce a grounded answer from a question and retrieved contexts."""

    async def generate(self, question: str, contexts: list[dict]) -> str:
        ...


class AnthropicGenerator:
    """Claude-backed answer generation.

    Construct with a configured client so credentials live in config:

        import anthropic
        gen = AnthropicGenerator(anthropic.AsyncAnthropic(api_key=...), model=...)
    """

    def __init__(self, client, model: str = "claude-sonnet-4-6", max_tokens: int = 1024):
        self._client = client
        self.model = model
        self.max_tokens = max_tokens

    async def generate(self, question: str, contexts: list[dict]) -> str:
        with metrics.timed_call("anthropic", "generate"):
            message = await self._client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=SYSTEM_PROMPT,
                messages=[
                    {"role": "user", "content": build_user_message(question, contexts)}
                ],
            )
        return message.content[0].text


class FakeGenerator:
    """Deterministic generator for tests — echoes how many contexts it saw."""

    def __init__(self, answer: str | None = None):
        self._answer = answer

    async def generate(self, question: str, contexts: list[dict]) -> str:
        if self._answer is not None:
            return self._answer
        if not contexts:
            return "The available documents do not contain enough information to answer that."
        return f"Answer to {question!r} from {len(contexts)} excerpt(s)."
