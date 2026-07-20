# DocStream — Distributed AI Document Pipeline

An **event-driven, distributed** document-intelligence platform. Documents are
ingested through a **FastAPI** gateway, streamed over **Apache Kafka** to
independently scalable **Python microservices** that extract text, generate
**vector embeddings**, and run **LLM** classification and summarisation, then
served back through a **CQRS** read API that answers questions with grounded,
cited **RAG** responses.

Runs on **Kubernetes** via a **Helm** chart, with **PostgreSQL**, **Qdrant**,
and **S3/MinIO** for state, and **Prometheus + Grafana** for observability.
Built around the distributed-systems patterns that make async pipelines
survivable: the **transactional outbox**, **idempotent consumers**,
**retries with dead-letter queues**, and **eventual consistency** between
write and read models.

**Stack:** Python · FastAPI · Apache Kafka (KRaft) · PostgreSQL · SQLAlchemy ·
Alembic · Qdrant · S3/MinIO · Redis · OpenAI embeddings · Anthropic Claude ·
Docker · Kubernetes · Helm · kind · Prometheus · Grafana · GitHub Actions ·
pytest · testcontainers · uv

---

## What it does

Submit a document (PDF or text) to the API. It is queued, text-extracted,
chunked, embedded into a vector database, classified and summarised by an LLM,
and projected into a read model. You can then run semantic search across your
corpus, or ask questions in natural language and get answers grounded in your
own documents with citations back to the source chunk.

Every stage is a separate deployable service communicating only through events.
Nothing blocks the caller: ingestion returns immediately with a job id.

---

## Architecture

The system is split **CQRS**-style — a write path that ingests and enriches, and
a read path that serves queries. They share no database tables and meet only at
the event log.

```
   COMMAND SIDE (write)                                  QUERY SIDE (read)
   ────────────────────                                  ─────────────────

           ┌─────────────┐   documents.ingested   ┌───────────────────┐
  Client ─▶│ API Gateway │ ─────────────────────▶ │ Extraction Worker │
   POST    │  (FastAPI)  │       (Kafka)          │   text / OCR      │
           └──────┬──────┘                        └─────────┬─────────┘
                  │ job row + outbox row                    │ documents.extracted
                  │ in ONE transaction                      ▼
                  ▼                              ┌──────────────────────┐
           ┌─────────────┐                       │  Enrichment Worker   │
           │ PostgreSQL  │◀──────────────────────│  chunk → embed →     │
           │             │                       │  classify + summarise│
           │  jobs       │                       └───┬──────────────┬───┘
           │  outbox     │                           │              │
           │  processed_ │              documents.   │              ▼
           │   events    │               enriched    │        ┌──────────┐
           │  document_  │                           │        │  Qdrant  │
           │   view ◀────┼──────┐                    │        │ (vectors)│
           └─────────────┘      │                    ▼        └────┬─────┘
                                │           ┌────────────────┐     │
                       ┌────────┴───────┐   │  DLQ topics    │     │
                       │   Projector    │   │  (poison msgs) │     │
                       │ builds read    │   └────────────────┘     │
                       │ model          │                          │
                       └────────────────┘                          │
                                                                   │
           ┌─────────────┐   reads view + vectors                  │
           │  Query API  │◀────────────────────────────────────────┘
           │  (FastAPI)  │   GET /search  ·  POST /ask  (RAG)
           └─────────────┘

   Object storage (S3/MinIO) holds document bytes — every service resolves
   URIs independently, so no pod shares a filesystem.

   All services: Docker ▶ Kubernetes (Helm) ▶ Prometheus + Grafana
```

**Write path.** The gateway persists a job row and stages its event in the
**same database transaction** (transactional outbox), so a job can never exist
without its event, or vice versa. A relay drains the outbox to Kafka. The
extraction worker pulls text and emits the next event; the enrichment worker
chunks and embeds it into Qdrant, runs the LLM, and emits `documents.enriched`.
Every service emits events the same reliable way.

**Read path.** A projector consumes `documents.enriched` and maintains a
denormalized `document_view` — the only writer of that table. The Query API
serves from that view plus Qdrant and never touches the `jobs` write model, so
read traffic scales independently of ingestion. The trade-off is deliberate
**eventual consistency**: a document is queryable moments after enrichment.

---

## Distributed-systems patterns

| Pattern | Implementation |
|---|---|
| **Transactional outbox** | `db/outbox.py` — job row + outbox row committed atomically; `gateway/relay.py` publishes to Kafka |
| **Idempotent consumers** | `db/idempotency.py` — `(event_id, consumer_group)` claimed inside the handler's transaction via SAVEPOINT. A failed handler rolls the claim back, so retries still work |
| **Retry + dead-letter queue** | `common/retry.py` — failures re-publish to the source topic with `attempt+1`; exhausted events route to `<topic>.DLQ` |
| **Exactly-once effects** | At-least-once Kafka delivery + idempotent handlers + deterministic UUIDv5 vector ids, so replays overwrite in place instead of duplicating |
| **CQRS** | Separate write model (`jobs`) and read model (`document_view`), joined only by events |
| **Eventual consistency** | Read model trails the pipeline by the projector's lag — measured, not assumed |
| **Stateless services** | Object storage for document bytes; any pod resolves any URI, so pods schedule anywhere |
| **Horizontal autoscaling** | HPA on the query service — the read path scales on its own |

---

## Event contract

Defined once in [`src/docstream/common`](src/docstream/common) so producers and
consumers cannot drift.

- **Topics** (`topics.py`): `documents.ingested` → `documents.extracted` →
  `documents.enriched`, each with a `.DLQ` twin.
- **Events** (`events.py`): every message is an `EventEnvelope` carrying
  `event_id` (dedup key), `correlation_id` (tracing), `attempt` (retry/DLQ
  policy), and a typed, discriminated payload.
- **Config** (`config.py`): 12-factor settings from environment variables.

---

## Observability

Prometheus scrapes every service; Grafana ships a provisioned dashboard
(`deploy/helm/docstream/templates/dashboard.yaml`) built around four questions:

- **CQRS lag** — Kafka consumer group lag, i.e. how stale the read model is.
  Reported by an external `kafka-exporter` rather than self-reported, because a
  wedged consumer stops updating its own metrics exactly when lag matters most.
- **AI latency and cost** — embedding, LLM, and vector-store calls timed
  separately, with p95/p99 and provider error rates.
- **Retrieval quality** — similarity-score distribution of the best hit per
  query, and how often the model declines because nothing cleared the relevance
  cutoff.
- **Orchestration** — pod restarts, HPA current/desired replicas, readiness.

Plus retry, dead-letter, and deduplication counters — visible proof the
fault-tolerance patterns are working.

---

## Quick start

**Prerequisites:** [Docker](https://docs.docker.com/get-docker/),
[uv](https://docs.astral.sh/uv/), and API keys for
[OpenAI](https://platform.openai.com) (embeddings) and
[Anthropic](https://console.anthropic.com) (LLM).

### Option 1 — Local processes

```bash
make install                       # uv sync
make up                            # Kafka, Postgres, Redis, Qdrant, MinIO, kafka-ui
make topics
cp .env.example .env               # then add your API keys
uv run alembic upgrade head
```

Run the five services, one terminal each:

```bash
make gateway      # :8000  ingest API (+ in-process outbox relay)
make worker       #        extraction worker
make enrichment   #        enrichment worker (embeddings + LLM)
make projector    #        CQRS read-model projector
make query        # :8001  query API (search + RAG)
```

### Option 2 — Everything in Docker

```bash
export DOCSTREAM_EMBEDDING__API_KEY=sk-...
export DOCSTREAM_LLM__API_KEY=sk-ant-...
make build && make up-all
```

### Option 3 — Kubernetes (kind)

```bash
make kind-up                       # 3-node cluster
make kind-load                     # build + load all 7 images
export DOCSTREAM_EMBEDDING__API_KEY=sk-... DOCSTREAM_LLM__API_KEY=sk-ant-...
make helm-install                  # migrations + topics run as bootstrap jobs
make k8s-status
make k8s-forward                   # gateway :8000, query :8001
make k8s-observability             # Grafana :3000, Prometheus :9090
```

The Helm chart bundles dev infrastructure (Postgres, Kafka, Qdrant, MinIO) so a
single command brings up a working system. For a real cluster, set
`infra.enabled=false` and point `external.*` at managed services.

---

## Using it

```bash
# Ingest — returns immediately with a job id
curl -F "file=@lease.pdf" http://localhost:8000/documents

# Track it: pending → extracting → extracted → enriching → completed
curl http://localhost:8000/jobs/<job_id>

# Read side: what has been indexed
curl http://localhost:8001/documents

# Semantic search — matching excerpts, no LLM
curl "http://localhost:8001/search?q=security+deposit&limit=3"

# RAG — grounded answer plus the sources it used
curl -X POST http://localhost:8001/ask \
  -H 'Content-Type: application/json' \
  -d '{"question":"What is the security deposit and when is it due?"}'
```

`/ask` returns the answer **and** the excerpts it was grounded in, so callers
can verify the grounding rather than trust it.

---

## Testing

```bash
make test              # unit: fully faked, no Docker, seconds
make test-integration  # Docker-backed: real Kafka, Postgres, Qdrant, MinIO
make ci                # everything CI runs, locally
```

The integration suite exists because fakes only assert what you *believe* an API
does. It exercises the real clients against throwaway containers
(testcontainers): Qdrant's point-id and query contracts, the full Alembic
migration chain on real Postgres, an end-to-end ingest → enrich → project →
search flow, and envelope round-tripping through a real broker.

CI additionally lints, validates the Helm chart, builds all seven images, and
deploys to a kind cluster asserting **zero pod restarts** — not merely eventual
readiness, since a crash-looping deployment can still converge.

---

## Layout

```
doc-stream/
├── src/docstream/
│   ├── common/           # event contract: envelopes, topics, config,
│   │                     # retry/DLQ policy, health, metrics
│   ├── db/               # models, transactional outbox, idempotency guard
│   ├── storage/          # pluggable object storage (local | S3/MinIO)
│   ├── gateway/          # write API + outbox relay
│   ├── extraction/       # ingested  → extracted
│   ├── enrichment/       # extracted → Qdrant + LLM → enriched
│   ├── projection/       # enriched  → CQRS read model
│   └── query/            # read API: retrieval, prompts, generation
├── deploy/
│   ├── helm/docstream/   # chart: services, HPA, bootstrap jobs, observability
│   └── kind/             # local cluster config
├── alembic/              # database migrations
├── tests/                # unit + Docker-backed integration suites
├── .github/workflows/    # CI and image publishing
├── Dockerfile            # multi-stage, one image per service
└── docker-compose*.yml   # local infrastructure and app overlay
```

Run `make help` to see every target.
