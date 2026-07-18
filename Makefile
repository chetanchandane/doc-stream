.DEFAULT_GOAL := help

.PHONY: help install up down logs topics ps test lint fmt clean gateway worker enrichment

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install: ## Sync Python deps with uv (incl. dev group)
	uv sync

up: ## Start local infra (Kafka, Postgres, Redis, Qdrant, kafka-ui)
	docker compose up -d

down: ## Stop local infra
	docker compose down

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

test: ## Run the test suite
	uv run pytest

lint: ## Lint with ruff
	uv run ruff check .

fmt: ## Format with ruff
	uv run ruff format .

clean: ## Remove local data volumes and caches
	docker compose down -v
	rm -rf .pytest_cache .ruff_cache .mypy_cache
