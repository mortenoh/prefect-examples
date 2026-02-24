# Getting Started

## Prerequisites

- **Python 3.13+**
- **[uv](https://docs.astral.sh/uv/)** -- fast Python package manager

## Installation

```bash
git clone https://github.com/mortenoh/prefect-examples.git
cd prefect-examples
make sync
```

`make sync` runs `uv sync`, which installs all runtime and development
dependencies (Prefect, pytest, ruff, mypy, mkdocs) into a project-local
virtual environment.

## Run your first flow

```bash
uv run python flows/basics/basics_hello_world.py
```

You should see output like:

```
Hello from Prefect!
Tue Jan 15 10:30:00 UTC 2025
```

Every flow file is self-contained with an `if __name__ == "__main__"` block, so
you can run any of them the same way:

```bash
uv run python flows/basics/basics_taskflow_etl.py
uv run python flows/basics/basics_dynamic_tasks.py
```

## Run the tests

```bash
make test
```

This runs the full pytest suite. All 110+ flows have corresponding tests in
`tests/`.

## Serve the documentation

```bash
make docs
```

Opens a local MkDocs server at `http://127.0.0.1:8000`.

## Run the Docker stack

For a full Prefect environment with a server, worker, and database:

```bash
make start
```

This runs `docker compose up --build` and starts PostgreSQL, Prefect Server
(UI at `http://localhost:4200`), a Prefect Worker, and RustFS for object
storage. Once running, deploy the example flows:

```bash
make deploy
```

See [Infrastructure](infrastructure.md) for full details on the Docker stack.

## Project layout

```
prefect-examples/
    flows/              # 111 example flows in topic subdirectories
    deployments/        # Production deployment examples
        dhis2_connection/   # Connection check deployment
        dhis2_ou/           # Org unit listing deployment
    src/
        prefect_examples/
            config.py   # Shared configuration defaults
            dhis2.py    # DHIS2 credentials block and API client
            tasks.py    # Reusable task library
    tests/              # pytest test suite
    docs/               # MkDocs documentation source
    compose.yml         # Docker Compose stack (Postgres, Server, Worker, RustFS)
    Makefile            # Common commands (sync, test, lint, docs, start, deploy)
    pyproject.toml      # Project metadata and tool config
    mkdocs.yml          # MkDocs configuration
```
