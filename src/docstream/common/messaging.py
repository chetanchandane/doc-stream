"""Thin aiokafka producer wrapper shared by the relay and (later) the workers.

Keeps a single async producer per process and exposes a ``publish`` that takes
the fields the outbox stores. The producer is configured for durability
(``acks=all`` + idempotence) so a publish that returns has really landed.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord

from docstream.common.config import KafkaSettings


class KafkaProducer:
    def __init__(self, settings: KafkaSettings) -> None:
        self._settings = settings
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        if self._producer is not None:
            return
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.bootstrap_servers,
            client_id=self._settings.client_id,
            acks=self._settings.acks,
            enable_idempotence=self._settings.enable_idempotence,
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def publish(self, topic: str, value: bytes, key: bytes | None = None) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaProducer.start() must be called before publish().")
        await self._producer.send_and_wait(topic, value=value, key=key)

    async def __aenter__(self) -> KafkaProducer:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()


class KafkaConsumer:
    """aiokafka consumer configured for at-least-once processing.

    Auto-commit is off: the caller commits *after* a record is successfully
    handled, so a crash mid-processing re-delivers rather than silently drops.
    Combined with idempotent handlers (Week 2), that gives exactly-once effects.
    """

    def __init__(
        self,
        settings: KafkaSettings,
        *,
        topics: tuple[str, ...],
        group_id: str,
        auto_offset_reset: str = "earliest",
    ) -> None:
        self._settings = settings
        self._topics = topics
        self._group_id = group_id
        self._auto_offset_reset = auto_offset_reset
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        if self._consumer is not None:
            return
        self._consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=self._settings.bootstrap_servers,
            group_id=self._group_id,
            client_id=self._settings.client_id,
            enable_auto_commit=False,
            auto_offset_reset=self._auto_offset_reset,
        )
        await self._consumer.start()

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

    def __aiter__(self) -> AsyncIterator[ConsumerRecord]:
        if self._consumer is None:
            raise RuntimeError("KafkaConsumer.start() must be called before iterating.")
        return self._consumer.__aiter__()

    async def commit(self) -> None:
        if self._consumer is None:
            raise RuntimeError("KafkaConsumer.start() must be called before commit().")
        await self._consumer.commit()

    async def __aenter__(self) -> KafkaConsumer:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()
