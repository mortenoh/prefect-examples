.DEFAULT_GOAL := help

.PHONY: help sync lint fmt test clean run

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
