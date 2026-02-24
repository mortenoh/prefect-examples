# Getting Started

## Prerequisites

- **Python 3.13+**
- **[uv](https://docs.astral.sh/uv/)** -- fast Python package manager

## Installation

```bash
git clone https://github.com/morteoh/prefect-examples.git
cd prefect-examples
make sync
```

`make sync` runs `uv sync`, which installs all runtime and development
dependencies (Prefect, pytest, ruff, mypy, mkdocs) into a project-local
virtual environment.

## Run your first flow

```bash
uv run python flows/001_hello_world.py
```

You should see output like:

```
Hello from Prefect!
Tue Jan 15 10:30:00 UTC 2025
```

Every flow file is self-contained with an `if __name__ == "__main__"` block, so
you can run any of them the same way:

```bash
uv run python flows/004_taskflow_etl.py
uv run python flows/010_dynamic_tasks.py
```

## Run the tests

```bash
make test
```

This runs the full pytest suite. All 20 flows have corresponding tests in
`tests/`.

## Serve the documentation

```bash
make docs
```

Opens a local MkDocs server at `http://127.0.0.1:8000`.

## Project layout

```
prefect-examples/
    flows/              # All 20 example flows (001-020)
    src/
        prefect_examples/
            config.py   # Shared configuration defaults
            tasks.py    # Reusable task library
    tests/              # pytest test suite
    docs/               # MkDocs documentation source
    Makefile            # Common commands (sync, test, lint, docs)
    pyproject.toml      # Project metadata and tool config
    mkdocs.yml          # MkDocs configuration
```
