.DEFAULT_GOAL := help

.PHONY: help sync lint fmt test clean run server start docs docs-build

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

sync: ## Install dependencies
	uv sync

lint: ## Run ruff check and mypy
	uv run ruff check .
	uv run mypy src/

fmt: ## Auto-format with ruff
	uv run ruff format .
	uv run ruff check --fix .

test: ## Run tests
	uv run pytest

clean: ## Remove build artifacts
	rm -rf __pycache__ .mypy_cache .pytest_cache .ruff_cache dist build *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

run: ## Run flow 001
	uv run python flows/001_hello_world.py

server: ## Start Prefect UI server (http://127.0.0.1:4200)
	PREFECT_SERVER_ANALYTICS_ENABLED=false PREFECT_SERVER_UI_SHOW_PROMOTIONAL_CONTENT=false uv run prefect server start

start: ## Start Prefect stack (PostgreSQL + Server + Worker + RustFS)
	docker compose up

docs: ## Serve docs locally
	uv run mkdocs serve

docs-build: ## Build static docs site
	uv run mkdocs build
