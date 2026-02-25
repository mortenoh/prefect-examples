-include .env
export

PREFECT_API_URL ?= http://localhost:4200/api
PREFECT_SERVER_ANALYTICS_ENABLED ?= false
PREFECT_SERVER_UI_SHOW_PROMOTIONAL_CONTENT ?= false

.DEFAULT_GOAL := help

.PHONY: help sync lint fmt test clean run server start restart deploy-local deploy register-blocks create-blocks generate-prefect-yaml docs docs-build

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

sync: ## Install dependencies
	uv sync

lint: ## Run ruff check and mypy
	uv run ruff check .
	uv run mypy src/ packages/ flows/ tests/

fmt: ## Auto-format with ruff
	uv run ruff format .
	uv run ruff check --fix .

test: ## Run tests
	uv run pytest

clean: ## Remove build artifacts
	rm -rf __pycache__ .mypy_cache .pytest_cache .ruff_cache dist build *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

run: ## Run flow basics_hello_world
	PREFECT_API_URL= uv run python flows/basics/basics_hello_world.py

server: ## Start Prefect UI server (http://127.0.0.1:4200)
	uv run prefect server start

start: ## Start Prefect stack (PostgreSQL + Server + Worker + RustFS)
	docker compose up --build

restart: ## Tear down, rebuild, and start the Docker stack from scratch
	docker compose down -v && docker compose build --no-cache && docker compose up

generate-prefect-yaml: ## Regenerate prefect.yaml from flows/ directory
	uv run python scripts/generate_prefect_yaml.py

deploy-local: ## Deploy all flows to local Prefect server (run after make server)
	uv run prefect work-pool create default --type process 2>/dev/null || true
	uv run prefect deploy --all

deploy: register-blocks create-blocks ## Register blocks, create instances, and deploy all flows
	cd deployments/dhis2_connection && uv run prefect deploy --all
	cd deployments/dhis2_ou && uv run prefect deploy --all
	cd deployments/dhis2_block_connection && uv run prefect deploy --all
	cd deployments/s3_parquet_export && uv run prefect deploy --all
	cd deployments/dhis2_geoparquet_export && uv run prefect deploy --all

register-blocks: ## Register custom block types with Prefect server
	uv run prefect block register -m prefect_dhis2

create-blocks: ## Create DHIS2 credentials block instances for all known servers
	uv run python scripts/create_blocks.py

docs: ## Serve docs locally
	uv run mkdocs serve

docs-build: ## Build static docs site
	uv run mkdocs build
