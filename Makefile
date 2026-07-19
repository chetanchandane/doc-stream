.DEFAULT_GOAL := help

.PHONY: ci help install up down build up-all down-all logs logs-app topics ps ps-all test test-integration test-all lint fmt clean gateway worker enrichment projector query kind-up kind-down kind-load helm-lint helm-template helm-install helm-uninstall k8s-status k8s-forward k8s-logs

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

## --- Kubernetes (kind + Helm) ------------------------------------------------
KIND_CLUSTER := docstream
CHART        := deploy/helm/docstream
RELEASE      := docstream
IMAGES       := gateway extraction enrichment projector query migrate topics

kind-up: ## Create the local kind cluster
	kind create cluster --config deploy/kind/kind-config.yaml

kind-down: ## Delete the kind cluster
	kind delete cluster --name $(KIND_CLUSTER)

kind-load: build ## Load locally-built images into the kind nodes
	@for img in $(IMAGES); do \
		echo "loading docstream/$$img:dev"; \
		kind load docker-image docstream/$$img:dev --name $(KIND_CLUSTER); \
	done

helm-lint: ## Lint the chart
	helm lint $(CHART)

helm-template: ## Render the chart locally (no cluster needed)
	helm template $(RELEASE) $(CHART)

helm-install: ## Install/upgrade the chart into kind (keys read from the environment)
	@test -n "$$DOCSTREAM_EMBEDDING__API_KEY" || { echo "set DOCSTREAM_EMBEDDING__API_KEY"; exit 1; }
	@test -n "$$DOCSTREAM_LLM__API_KEY" || { echo "set DOCSTREAM_LLM__API_KEY"; exit 1; }
	@# Piped via stdin, NOT --set: keeps the keys out of shell history, the
	@# process list, and any CI log that echoes the command.
	@printf 'secrets:\n  embeddingApiKey: "%s"\n  llmApiKey: "%s"\n' \
		"$$DOCSTREAM_EMBEDDING__API_KEY" "$$DOCSTREAM_LLM__API_KEY" \
		| helm upgrade --install $(RELEASE) $(CHART) -f - --wait --timeout 10m

helm-uninstall: ## Remove the release
	helm uninstall $(RELEASE)

k8s-status: ## Pods and services for the release
	kubectl get pods,svc -l app.kubernetes.io/instance=$(RELEASE)

k8s-forward: ## Port-forward both APIs (gateway :8000, query :8001)
	@echo "gateway -> http://localhost:8000 ; query -> http://localhost:8001 (Ctrl-C to stop)"
	kubectl port-forward svc/docstream-gateway 8000:8000 & \
	kubectl port-forward svc/docstream-query 8001:8001 & \
	wait

k8s-logs: ## Tail logs from every app pod
	kubectl logs -l app.kubernetes.io/part-of=docstream --all-containers --tail=100 -f

test: ## Run the unit test suite (fast, no Docker)
	uv run pytest

test-integration: ## Run Docker-backed integration tests (real Kafka/Postgres/Qdrant)
	uv run pytest -m integration -v

test-all: ## Run unit + integration tests
	uv run pytest -m "integration or not integration" -v

lint: ## Lint with ruff
	uv run ruff check .

ci: ## Run everything CI runs, locally (except the kind e2e)
	uv lock --check
	uv run ruff check .
	uv run pytest -q
	uv run pytest -m integration -v
	helm lint $(CHART)
	helm template $(RELEASE) $(CHART) > /dev/null

fmt: ## Format with ruff
	uv run ruff format .

clean: ## Remove local data volumes and caches
	docker compose down -v
	rm -rf .pytest_cache .ruff_cache .mypy_cache
