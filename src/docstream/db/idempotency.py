"""Idempotent-consumption guard.

``mark_processed`` claims an event for a consumer group by inserting a
``processed_events`` row. It runs inside a SAVEPOINT (nested transaction) so a
duplicate — which trips the unique constraint — rolls back only the insert,
leaving the caller's outer transaction usable. Call it at the top of a handler,
inside the handler's transaction: if it returns ``False`` the event was already
processed and the handler should return without doing the work.

Because the claim shares the handler's transaction, it commits with the work (on
success) or rolls back with it (on failure) — so a failed handler leaves the
event un-claimed and safe to retry.
"""

from __future__ import annotations

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from docstream.common import metrics
from docstream.db.models import ProcessedEvent


async def mark_processed(
    session: AsyncSession, event_id: str, consumer_group: str
) -> bool:
    """Claim ``event_id`` for ``consumer_group``.

    Returns ``True`` if this is the first time the group has seen the event
    (caller should do the work), ``False`` if it was already processed (caller
    should skip). Does not commit — the caller owns the transaction boundary.
    """
    try:
        async with session.begin_nested():  # SAVEPOINT
            session.add(
                ProcessedEvent(event_id=event_id, consumer_group=consumer_group)
            )
            await session.flush()
        return True
    except IntegrityError:
        # Unique (event_id, consumer_group) violated -> already processed.
        # Counted here because this is the single place dedup is decided, so
        # every worker gets the metric for free.
        metrics.events_processed_total.labels(
            stage=consumer_group, result="duplicate"
        ).inc()
        return False
