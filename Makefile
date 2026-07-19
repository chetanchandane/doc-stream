.DEFAULT_GOAL := help

.PHONY: help install up down build up-all down-all logs logs-app topics ps ps-all test test-integration test-all lint fmt clean gateway worker enrichment projector query

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install: ## Sync Python deps with uv (incl. dev group)
	uv sync

COMPOSE_ALL := docker compose -f docker-compose.yml -f docker-compose.app.yml

up: ## Start local infra (Kafka, Postgres, Redis, Qdrant, kafka-ui)
	docker compose up -d

down: ## Stop local infra
	docker compose down

build: ## Build all five service images
	$(COMPOSE_ALL) build

up-all: ## Start infra AND all app services in containers
	$(COMPOSE_ALL) up -d

down-all: ## Stop everything (infra + app services)
	$(COMPOSE_ALL) down

logs-app: ## Tail logs from the app services
	$(COMPOSE_ALL) logs -f gateway extraction enrichment projector query

ps-all: ## Status of every container
	$(COMPOSE_ALL) ps

logs: ## Tail infra logs
	docker compose logs -f

ps: ## Show infra container status
	docker compose ps

topics: ## Create the DocStream Kafka topics
	uv run python scripts/create_topics.py

gateway: ## Run the API Gateway (with in-process outbox relay)
	uv run uvicorn docstream.gateway.app:app --reload

worker: ## Run the extraction worker
	uv run python -m docstream.extraction.worker

enrichment: ## Run the enrichment worker
	uv run python -m docstream.enrichment.worker

projector: ## Run the read-model projector (CQRS read side)
	uv run python -m docstream.projection.worker

query: ## Run the Query API (read side) on port 8001
	uv run uvicorn docstream.query.app:app --reload --port 8001

test: ## Run the unit test suite (fast, no Docker)
	uv run pytest

test-integration: ## Run Docker-backed integration tests (real Kafka/Postgres/Qdrant)
	uv run pytest -m integration -v

test-all: ## Run unit + integration tests
	uv run pytest -m "integration or not integration" -v

lint: ## Lint with ruff
	uv run ruff check .

fmt: ## Format with ruff
	uv run ruff format .

clean: ## Remove local data volumes and caches
	docker compose down -v
	rm -rf .pytest_cache .ruff_cache .mypy_cache
