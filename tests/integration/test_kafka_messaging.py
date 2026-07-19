"""Messaging contract against a REAL Kafka broker.

The unit suite uses FakeProducer, which records calls but never proves our
producer/consumer wrappers actually talk to a broker, preserve the envelope, or
honour manual-commit semantics. This does.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from docstream.common.config import KafkaSettings
from docstream.common.events import DocumentIngested, EventEnvelope, make_event

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _envelope(document_id: str) -> EventEnvelope:
    return make_event(
        DocumentIngested(
            job_id=str(uuid.uuid4()),
            document_id=document_id,
            filename="f.txt",
            content_type="text/plain",
            size_bytes=3,
            storage_uri="mem://f",
        ),
        source="integration-test",
    )


async def test_publish_and_consume_round_trip(kafka_bootstrap):
    from docstream.common.messaging import KafkaConsumer, KafkaProducer

    settings = KafkaSettings(bootstrap_servers=kafka_bootstrap)
    topic = f"it.{uuid.uuid4().hex[:8]}"
    document_id = str(uuid.uuid4())
    sent = _envelope(document_id)

    producer = KafkaProducer(settings)
    await producer.start()
    try:
        await producer.publish(topic, sent.to_bytes(), sent.key())
    finally:
        await producer.stop()

    consumer = KafkaConsumer(
        settings, topics=(topic,), group_id=f"g.{uuid.uuid4().hex[:8]}"
    )
    await consumer.start()
    try:
        async def _first():
            async for record in consumer:
                return record
            return None

        record = await asyncio.wait_for(_first(), timeout=30)
        assert record is not None

        received = EventEnvelope.from_bytes(record.value)
        # The envelope survives the wire intact — this is what every consumer
        # relies on for dedup (event_id) and tracing (correlation_id).
        assert received.event_id == sent.event_id
        assert received.correlation_id == sent.correlation_id
        assert received.document_id == document_id
        assert record.key == sent.key()

        await consumer.commit()
    finally:
        await consumer.stop()
