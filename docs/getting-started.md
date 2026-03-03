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

## Environment variables

Some flows require credentials or API URLs. Copy the example file and fill in
any values you need:

```bash
cp .env.example .env
```

Every flow file calls `load_dotenv()` in its `if __name__ == "__main__"` block,
so variables from `.env` are loaded automatically when you run a flow with
`uv run python`.

### Available variables

| Variable | Required by | Description |
|----------|-------------|-------------|
| `DHIS2_BASE_URL` | DHIS2 import flows | DHIS2 instance URL |
| `DHIS2_USERNAME` | DHIS2 import flows | DHIS2 username |
| `DHIS2_PASSWORD` | DHIS2 import flows | DHIS2 password |
| `CDSAPI_KEY` | ERA5 climate flows | CDS API key from your [CDS profile](https://cds.climate.copernicus.eu/profile) |
| `SLACK_WEBHOOK_URL` | Notification flows | Slack incoming webhook URL |
| `PREFECT_SERVER_API_AUTH_STRING` | Docker stack | Basic auth for Prefect server (`user:pass`) |
| `PREFECT_API_AUTH_STRING` | Docker stack | Basic auth for Prefect CLI/worker (`user:pass`) |
| `POSTGRES_PASSWORD` | Docker stack | PostgreSQL password (default: `prefect`) |
| `RUSTFS_ACCESS_KEY` | Docker stack | RustFS/S3 access key (default: `admin`) |
| `RUSTFS_SECRET_KEY` | Docker stack | RustFS/S3 secret key (default: `admin`) |

`CDSAPI_URL` defaults to `https://cds.climate.copernicus.eu/api` and does not
need to be set. See [ERA5-Land](era5-land.md#cds-api-setup) for CDS
registration steps and [Infrastructure](infrastructure.md#docker-deployment-requirements)
for per-deployment requirements.

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

This runs the full pytest suite. All 134 flows have corresponding tests in
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
    flows/              # 113 example flows in topic subdirectories
    deployments/        # Production deployment examples
        dhis2_connection/       # Connection check deployment
        dhis2_ou/               # Org unit listing deployment
        dhis2_block_connection/ # Block-based connection deployment
        s3_parquet_export/      # S3 parquet export deployment
        dhis2_geoparquet_export/ # DHIS2 GeoParquet export deployment
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
