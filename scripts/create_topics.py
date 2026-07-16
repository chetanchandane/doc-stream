"""Create the DocStream Kafka topics.

Auto-creation is disabled in docker-compose on purpose, so the topic set stays
intentional. Run after `docker compose up -d`:

    make topics
    # or
    uv run python scripts/create_topics.py

Idempotent: existing topics are left alone.
"""

from __future__ import annotations

import asyncio
import sys

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

# Allow running as a plain script without installing the package first.
sys.path.insert(0, "src")

from docstream.common.config import get_settings  # noqa: E402
from docstream.common.topics import ALL_TOPICS  # noqa: E402

PARTITIONS = 3
REPLICATION = 1  # single broker locally


async def main() -> None:
    settings = get_settings()
    admin = AIOKafkaAdminClient(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        client_id=f"{settings.kafka.client_id}-admin",
    )
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        wanted = [t for t in ALL_TOPICS if t not in existing]
        if not wanted:
            print(f"All {len(ALL_TOPICS)} topics already exist. Nothing to do.")
            return

        new_topics = [
            NewTopic(name=t, num_partitions=PARTITIONS, replication_factor=REPLICATION)
            for t in wanted
        ]
        try:
            await admin.create_topics(new_topics)
        except TopicAlreadyExistsError:
            pass

        for t in wanted:
            print(f"  created  {t}")
        print(f"Done. {len(wanted)} created, {len(existing & set(ALL_TOPICS))} already present.")
    finally:
        await admin.close()


if __name__ == "__main__":
    asyncio.run(main())
