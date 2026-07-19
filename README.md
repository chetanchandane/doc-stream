# DocStream — Event-Driven AI Document Pipeline

An asynchronous, event-driven service that ingests documents through an API,
streams them through **Kafka** to worker services that run **LLM/RAG
enrichment**, stores results and vectors in **Postgres + Qdrant**, and runs on
**Kubernetes** with retries, dead-letter handling, autoscaling, and full
observability.

> Status: **Week 2 complete.** The pipeline runs end to end and is queryable.
> Submit a document and it flows API Gateway → outbox → `documents.ingested` →
> extraction → `documents.extracted` → enrichment (Qdrant vectors + LLM
> classification/summary) → `documents.enriched` → read-model projection. A
> separate Query API then serves semantic search and grounded, cited answers over
> your documents. Every consumer is idempotent, every stage retries and
> dead-letters, and the whole flow is covered by unit and Docker-backed
> integration tests. Kubernetes, Helm, and CI/CD land in Week 3.

## Architecture

The system is split **CQRS-style**: a write path that ingests and enriches, and a
read path that serves queries. They meet only at the event log.

```
  COMMAND SIDE (write)                                    QUERY SIDE (read)
  ────────────────────                                    ─────────────────
        ┌─────────────┐   documents.ingested   ┌───────────────────┐
Client ▶│ API Gateway │ ─────────────────────▶ │ Extraction Worker │
 (POST) │  (FastAPI)  │      (Kafka)           │   text / OCR      │
        └──────┬──────┘                        └─────────┬─────────┘
               │ job + outbox row                        │ documents.extracted
               │ (one transaction)                       ▼
               ▼                              ┌──────────────────────┐
        ┌─────────────┐                       │  Enrichment Worker   │
        │  PostgreSQL │◀──────────────────────│  chunk → embed →     │
        │  jobs       │                       │  classify + summarize│
        │  outbox     │                       └───────┬──────┬───────┘
        │  processed_ │                               │      │
        │   events    │                    documents. │      ▼
        │  document_  │                     enriched  │  ┌────────┐
        │   view ◀────┼───────┐                       │  │ Qdrant │
        └─────────────┘       │                       │  │vectors │
                              │                       ▼  └───┬────┘
                     ┌────────┴────────┐    ┌────────────┐   │
                     │    Projector    │◀───│ DLQ topics │   │
                     │ builds read model│    │ (failures) │   │
                     └─────────────────┘    └────────────┘   │
                                                             │
        ┌─────────────┐   reads view + vectors               │
        │  Query API  │◀─────────────────────────────────────┘
        │  (FastAPI)  │   GET /search · POST /ask (RAG)
        └─────────────┘
  All services: Docker ▶ Kubernetes (Helm) ▶ Prometheus + Grafana + OpenTelemetry
```

**Write flow:** the client submits a document to the API Gateway, which persists a
job row and stages an event in the same transaction (transactional outbox); a
relay drains it to Kafka. The extraction worker pulls text and emits the next
event. The enrichment worker chunks and embeds the text into Qdrant, runs LLM
classification and summarization, marks the job complete, and emits
`documents.enriched`. Every emit goes through the outbox, so all services produce
events the same reliable way. Failures retry on their own topic and dead-letter
after N attempts.

**Read flow:** a projector consumes `documents.enriched` and maintains the
`document_view` read model — the only writer of that table. The Query API serves
from that view plus Qdrant, and never touches the `jobs` write model, so read
traffic scales independently of ingest. The cost is eventual consistency: a
document becomes queryable moments after it's enriched.

## Distributed-systems patterns (the point of the project)

| Pattern | Where it lives |
|---|---|
| Transactional outbox | `db/outbox.py` — job row + `outbox` row in one transaction; `gateway/relay.py` publishes to Kafka |
| Idempotent consumers | `db/idempotency.py` — `(event_id, consumer_group)` claimed inside the handler's transaction via SAVEPOINT; a failed handler rolls the claim back so retries still work |
| Retry + dead-letter topic | `common/retry.py` — re-publish to the source topic with `attempt+1`; route to `<topic>.DLQ` after `max_attempts` |
| CQRS | `projection/worker.py` builds `document_view`; `query/` reads only that view + Qdrant, never the write model |
| Eventual consistency | the read model trails the pipeline by the projector's lag |
| Idempotent vector writes | deterministic UUIDv5 point ids, so a replayed event overwrites in place instead of duplicating |
| Consumer-lag autoscaling | KEDA on Kafka lag (Week 3) |
| Distributed tracing | OpenTelemetry across all services (Week 4) |

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
make install            # uv sync (installs the dev group too)

# 2. Start local infra (Kafka, Postgres, Redis, Qdrant, kafka-ui)
make up

# 3. Create the Kafka topics
make topics

# 4. Inspect topics in the browser
open http://localhost:8080     # kafka-ui
```

`cp .env.example .env` if you want to override any defaults. Run `make help` to
see every target.

Enrichment calls OpenAI (embeddings) and Anthropic (classification/summary), so
set those keys in `.env` before running the enrichment worker or Query API:

```bash
DOCSTREAM_EMBEDDING__API_KEY=sk-...
DOCSTREAM_LLM__API_KEY=sk-ant-...
```

### Run the services

Apply migrations once, then start the five services (one terminal each):

```bash
uv run alembic upgrade head

make gateway      # :8000  API Gateway (runs the outbox relay in-process)
make worker       #        extraction worker
make enrichment   #        enrichment worker (Qdrant + LLM)
make projector    #        read-model projector
make query        # :8001  Query API (search + RAG)
```

To run the relay as its own process instead, set
`DOCSTREAM_RELAY__RUN_IN_PROCESS=false` and run `python -m docstream.gateway.relay`.

### Ingest a document (write path)

```bash
curl -F "file=@/path/to/lease.pdf" http://localhost:8000/documents
# -> {"job_id": "...", "document_id": "...", "status": "pending"}

curl http://localhost:8000/jobs/<job_id>
```

Poll until `status` reaches `completed`, walking `pending → extracting →
extracted → enriching → completed`. The response also carries the enrichment
results (`classification`, `summary`, `chunk_count`). Watch the events flow
through kafka-ui at http://localhost:8080.

### Query it (read path)

```bash
# What's been indexed (the read model)
curl -s http://localhost:8001/documents | jq

# Semantic search — returns matching excerpts, no LLM
curl -s "http://localhost:8001/search?q=distributed%20systems&limit=3" | jq

# Grounded RAG answer — retrieves, then answers with citations
curl -s -X POST http://localhost:8001/ask \
  -H 'Content-Type: application/json' \
  -d '{"question":"What is the security deposit?","limit":5}' | jq
```

`/ask` returns the answer **and** the source excerpts it was grounded in, so you
can verify the grounding rather than trust it.

## Testing

```bash
make test              # unit suite: fast, fully faked, no Docker
make test-integration  # Docker-backed: real Kafka, Postgres, and Qdrant
make test-all          # both
```

The unit suite fakes Qdrant and the LLM providers so it runs anywhere in seconds.
The integration suite exists because fakes can only assert what you *believe* an
API does — it runs the real clients against throwaway containers (testcontainers),
covering Qdrant's point-id and query contracts, the full alembic migration chain
on real Postgres, an end-to-end ingest → enrich → project → search flow, and
envelope round-tripping through a real broker.

## Project layout

```
doc-stream/
├── docker-compose.yml         # local infra: Kafka, Postgres, Redis, Qdrant, UI
├── Makefile                   # install / up / topics / test / lint
├── pyproject.toml             # uv-managed deps (src layout)
├── alembic/                   # async migrations (0001..0005)
├── scripts/
│   └── create_topics.py       # idempotent topic bootstrap
├── src/docstream/
│   ├── common/                # the shared event contract
│   │   ├── config.py          # nested 12-factor settings
│   │   ├── events.py          # EventEnvelope + typed payloads
│   │   ├── messaging.py       # aiokafka producer/consumer wrappers
│   │   ├── retry.py           # retry + DLQ policy (shared by all workers)
│   │   └── topics.py
│   ├── db/                    # async SQLAlchemy
│   │   ├── base.py
│   │   ├── models.py          # Job, OutboxEvent, ProcessedEvent, DocumentView
│   │   ├── outbox.py          # transactional-outbox helpers
│   │   └── idempotency.py     # mark_processed dedup guard
│   ├── storage/               # raw-bytes storage (local FS for now)
│   ├── gateway/               # write API: ingest + job status, outbox relay
│   ├── extraction/            # consume ingested -> emit extracted
│   ├── enrichment/            # consume extracted -> Qdrant + LLM -> emit enriched
│   │   ├── chunking.py        # text splitting
│   │   ├── embedding.py       # Embedder protocol (OpenAI + fake)
│   │   ├── qdrant_store.py    # collection, deterministic upsert, search
│   │   ├── llm.py             # LLM protocol (Claude + fake)
│   │   └── worker.py
│   ├── projection/            # CQRS read side: enriched -> document_view
│   │   └── worker.py
│   └── query/                 # read API: search + grounded RAG answers
│       ├── retrieval.py       # embed query -> Qdrant search
│       ├── prompts.py         # grounded, cited answer prompt
│       ├── generation.py      # Generator protocol (Claude + fake)
│       ├── service.py         # transport-free orchestration
│       └── app.py             # GET /documents /search · POST /ask
└── tests/
    ├── ...                    # unit tests (faked deps, no Docker)
    └── integration/           # testcontainers: real Kafka/Postgres/Qdrant
```

## Roadmap

- **Week 1** — skeleton, event backbone, API Gateway + outbox, extraction worker (done).
- **Week 2** — enrichment worker (Qdrant + LLM), idempotent consumers, retry + DLQ,
  plus CQRS read side (projector + Query API with RAG) and a Docker-backed
  integration suite (done).
- **Week 3** — Docker images, Kubernetes + Helm, GitHub Actions, Prometheus + Grafana.
- **Week 4** — OpenTelemetry tracing, k6 load test, polish.
