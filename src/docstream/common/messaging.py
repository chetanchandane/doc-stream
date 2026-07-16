"""Thin aiokafka producer wrapper shared by the relay and (later) the workers.

Keeps a single async producer per process and exposes a ``publish`` that takes
the fields the outbox stores. The producer is configured for durability
(``acks=all`` + idempotence) so a publish that returns has really landed.
"""

from __future__ import annotations

from aiokafka import AIOKafkaProducer

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

    async def __aenter__(self) -> "KafkaProducer":
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()
