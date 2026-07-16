# DocStream — Event-Driven AI Document Pipeline

An asynchronous, event-driven service that ingests documents through an API,
streams them through **Kafka** to worker services that run **LLM/RAG
enrichment**, stores results and vectors in **Postgres + Qdrant**, and runs on
**Kubernetes** with retries, dead-letter handling, autoscaling, and full
observability.

> Status: **Week 1 — skeleton and the event backbone.** This commit sets up the
> repo, the event/topic contract, and the local infrastructure. The API
> Gateway, workers, and outbox relay land next.

## Architecture

```
        ┌─────────────┐     documents.ingested      ┌──────────────────┐
Client ─▶│ API Gateway │ ───────────────────────────▶│ Extraction Worker │
 (POST) │  (FastAPI)  │      (Kafka topic)          │     (Python)      │
        └──────┬──────┘                              └────────┬─────────┘
               │ writes job (outbox)                          │ documents.extracted
               ▼                                              ▼
        ┌─────────────┐                             ┌────────────────────┐
        │  PostgreSQL │                             │ AI Enrichment Worker│
        │ (job state) │                             │  embed + classify   │
        └─────────────┘                             │  + summarize (RAG)  │
               ▲                                     └─────────┬──────────┘
               │ documents.enriched                            │
        ┌──────┴──────┐                                        │
        │  Query API  │◀──── semantic search ───┐              │
        │  (FastAPI)  │                         ▼              ▼
        └─────────────┘                   ┌──────────┐   ┌───────────┐
                                          │  Qdrant  │   │  DLQ topic │
                                          │ (vectors)│   │ (failures) │
                                          └──────────┘   └───────────┘
  All services: Docker ▶ Kubernetes (Helm) ▶ Prometheus + Grafana + OpenTelemetry
```

**Flow:** the client submits a document to the API Gateway, which persists a job
row and emits an event (transactional outbox). An extraction worker consumes it,
pulls text (OCR if needed), and emits the next event. The enrichment worker
embeds the text into Qdrant, runs LLM classification/summarization/field
extraction, and emits an enriched event that updates the job. A query API serves
results and semantic search. Failures route to a dead-letter topic after retries.

## Distributed-systems patterns (the point of the project)

| Pattern | Where it lives |
|---|---|
| Transactional outbox | job row + `outbox` row in one transaction; a relay publishes to Kafka |
| Idempotent consumers | dedup on `event_id` via Redis `SETNX` / `processed_events` table |
| Retry + dead-letter topic | attempt counter in the envelope; route to `<topic>.DLQ` after N tries |
| Eventual consistency | job store vs. query/read model |
| Consumer-lag autoscaling | KEDA on Kafka lag (stretch) |
| Distributed tracing | OpenTelemetry across all services (stretch) |

## Stack

FastAPI · aiokafka · Apache Kafka (KRaft) · PostgreSQL + SQLAlchemy + Alembic ·
Qdrant · Redis · Docker · Kubernetes + Helm · Prometheus + Grafana ·
OpenTelemetry · GitHub Actions · pytest + testcontainers · uv · k6

## The event contract

Defined once in [`src/docstream/common`](src/docstream/common) so producers and
consumers can't drift:

- **Topics** (`topics.py`): `documents.ingested` → `documents.extracted` →
  `documents.enriched`, each with a `.DLQ` dead-letter twin.
- **Events** (`events.py`): every message is an `EventEnvelope` carrying
  `event_id` (dedup key), `correlation_id` (tracing), `attempt` (retry/DLQ), and
  a typed `payload`.
- **Config** (`config.py`): 12-factor settings from env vars / `.env`.

## Getting started

Prerequisites: [Docker](https://docs.docker.com/get-docker/) and
[uv](https://docs.astral.sh/uv/).

```bash
# 1. Install Python deps
make install            # uv sync --extra dev

# 2. Start local infra (Kafka, Postgres, Redis, Qdrant, kafka-ui)
make up

# 3. Create the Kafka topics
make topics

# 4. Inspect topics in the browser
open http://localhost:8080     # kafka-ui
```

`cp .env.example .env` if you want to override any defaults. Run `make help` to
see every target.

## Project layout

```
doc-stream/
├── docker-compose.yml         # local infra: Kafka, Postgres, Redis, Qdrant, UI
├── Makefile                   # install / up / topics / test / lint
├── pyproject.toml             # uv-managed deps (src layout)
├── scripts/
│   └── create_topics.py       # idempotent topic bootstrap
├── src/docstream/
│   └── common/                # the shared event contract
│       ├── config.py
│       ├── events.py
│       └── topics.py
└── tests/
    └── test_events.py
```

## Roadmap

- **Week 1** — skeleton + event backbone (this). API Gateway + outbox next.
- **Week 2** — AI enrichment worker, idempotency, retry + DLQ.
- **Week 3** — Kubernetes + Helm, GitHub Actions, Prometheus + Grafana.
- **Week 4** — OpenTelemetry tracing, k6 load test, polish.
