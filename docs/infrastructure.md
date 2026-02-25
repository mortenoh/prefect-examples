# Infrastructure

The project includes a Docker Compose stack for running a complete Prefect
environment locally. This page documents the services, configuration, and
development workflow.

---

## Quick start

```bash
make start          # start all services
make deploy         # register deployments with the Prefect server
```

The Prefect UI is available at `http://localhost:4200`.

---

## Services

The `compose.yml` defines four services:

```mermaid
graph LR
    W[Prefect Worker] -->|polls| S[Prefect Server :4200]
    S -->|reads/writes| P[(PostgreSQL :5432)]
    S -->|stores results| R[RustFS :9000]
```

### PostgreSQL

| Setting | Value |
|---|---|
| Image | `postgres:17-alpine` |
| Port | `5432` |
| User / Password / DB | `prefect` / `prefect` / `prefect` |

Stores Prefect server state (flow runs, deployments, blocks, etc.). A
healthcheck ensures the server waits for the database to be ready.

### Prefect Server

| Setting | Value |
|---|---|
| Image | `prefecthq/prefect:3-python3.13` |
| Port | `4200` (UI + API) |
| Depends on | PostgreSQL (healthy) |

Runs `prefect server start --host 0.0.0.0`. The server provides the UI, API,
and orchestration layer. It connects to PostgreSQL via the
`PREFECT_API_DATABASE_CONNECTION_URL` environment variable.

### Prefect Worker

| Setting | Value |
|---|---|
| Image | `prefecthq/prefect:3-python3.13` |
| Depends on | Prefect Server (healthy) |
| Work pool | `default` |

Runs `prefect worker start -p default`. The worker polls the server for
scheduled flow runs and executes them. It has access to the same volumes as
the server (`flows/`, `src/`, `deployments/`).

### RustFS

| Setting | Value |
|---|---|
| Image | `rustfs/rustfs:latest` |
| Ports | `9000` (API), `9001` (console) |
| Access Key / Secret Key | `prefect` / `prefect123` |

S3-compatible object storage for flow result persistence and artifact storage.

---

## Volumes

All services share host-mounted volumes so code changes are immediately
available without rebuilding containers:

| Host path | Container path | Purpose |
|---|---|---|
| `./flows` | `/opt/prefect/flows` | Flow source files |
| `./src` | `/opt/prefect/src` | Shared library (`prefect_examples`) |
| `./deployments` | `/opt/prefect/deployments` | Deployment configurations |

The `PYTHONPATH` environment variable is set to `/opt/prefect/src` so that
`from prefect_examples.dhis2 import ...` works inside containers.

---

## Environment variables

Shared across services via the `x-prefect-env` anchor:

| Variable | Value | Purpose |
|---|---|---|
| `PREFECT_API_DATABASE_CONNECTION_URL` | `postgresql+asyncpg://prefect:prefect@postgres:5432/prefect` | Database connection |
| `PREFECT_SERVER_ANALYTICS_ENABLED` | `false` | Disable telemetry |
| `PREFECT_SERVER_UI_SHOW_PROMOTIONAL_CONTENT` | `false` | Clean UI |
| `PYTHONPATH` | `/opt/prefect/src` | Make `prefect_examples` importable |

The worker additionally sets:

| Variable | Value | Purpose |
|---|---|---|
| `PREFECT_API_URL` | `http://prefect-server:4200/api` | Connect worker to server |

---

## Makefile targets

| Target | Command | Description |
|---|---|---|
| `make start` | `docker compose up --build` | Start all services (foreground) |
| `make restart` | `docker compose down -v && docker compose build --no-cache && docker compose up` | Full teardown and rebuild |
| `make deploy` | register-blocks + create-blocks + `prefect deploy --all` | Register blocks, create instances, and deploy all flows |
| `make register-blocks` | `prefect block register -m prefect_dhis2` | Register custom block types with the server |
| `make create-blocks` | `uv run python scripts/create_blocks.py` | Create DHIS2 credentials block instances for all known servers |
| `make server` | `uv run prefect server start` | Start a standalone server (no Docker) |

To run services in the background, add `-d`:

```bash
docker compose up --build -d
```

View logs:

```bash
docker compose logs -f prefect-server
docker compose logs -f prefect-worker
```

Stop and remove everything (including database data):

```bash
docker compose down -v
```

---

## Deployment workflow

### 1. Start the stack

```bash
make start
```

Wait for the healthchecks to pass. The Prefect UI will be available at
`http://localhost:4200`.

### 2. Deploy flows

```bash
make deploy
```

This registers the deployments from `deployments/dhis2_connection/` and
`deployments/dhis2_ou/` with the Prefect server. Each deployment has its own
`prefect.yaml` that defines the entrypoint, schedule, and work pool.

### 3. Verify in the UI

Open `http://localhost:4200` and navigate to the Deployments page. You should
see:

- **dhis2-connection** -- runs every 15 minutes
- **dhis2-ou** -- runs every 15 minutes

### 4. Trigger a manual run

```bash
PREFECT_API_URL=http://localhost:4200/api \
  uv run prefect deployment run dhis2_connection/dhis2-connection
```

Or use the "Run" button in the UI.

### 5. Monitor

The Flow Runs page in the UI shows run status, logs, artifacts, and task-level
details. Use the CLI for a quick check:

```bash
PREFECT_API_URL=http://localhost:4200/api uv run prefect flow-run ls
```

---

## Deployment configuration

Each deployment directory contains three files:

```
deployments/dhis2_ou/
    flow.py           # The flow entrypoint
    prefect.yaml      # Deployment configuration (schedule, work pool)
    deploy.py         # Optional programmatic deployment script
```

### `prefect.yaml` format

```yaml
pull:
  - prefect.deployments.steps.set_working_directory:
      directory: /opt/prefect/deployments/dhis2_ou

deployments:
  - name: dhis2-ou
    entrypoint: flow.py:dhis2_ou_flow
    schedules:
      - cron: "*/15 * * * *"
        timezone: "UTC"
    work_pool:
      name: default
```

**Key fields:**

| Field | Description |
|---|---|
| `pull` | Steps to prepare the execution environment (e.g. set working directory) |
| `name` | Deployment name (used in `prefect deployment run <flow>/<name>`) |
| `entrypoint` | `<file>:<function>` pointing to the flow |
| `schedules` | List of schedule objects (cron, interval, or rrule) |
| `schedules[].cron` | Cron expression (e.g. `"*/15 * * * *"` for every 15 minutes) |
| `schedules[].timezone` | Timezone for the schedule (e.g. `"UTC"`, `"Europe/Oslo"`) |
| `work_pool.name` | Which work pool executes the runs |

### Schedule types in `prefect.yaml`

```yaml
# Cron schedule
schedules:
  - cron: "0 6 * * *"
    timezone: "UTC"

# Interval schedule (seconds)
schedules:
  - interval: 900
    timezone: "UTC"

# RRule schedule
schedules:
  - rrule: "FREQ=WEEKLY;BYDAY=MO,WE,FR"
    timezone: "UTC"
```

Multiple schedules can be combined on a single deployment. Each schedule
creates independent runs.

### Updating schedules

After a deployment is registered, schedules can be changed without
redeploying:

```bash
# Change to daily at 08:00
prefect deployment set-schedule dhis2_ou/dhis2-ou --cron "0 8 * * *"

# Change to every 30 minutes
prefect deployment set-schedule dhis2_ou/dhis2-ou --interval 1800

# Remove all schedules
prefect deployment clear-schedule dhis2_ou/dhis2-ou

# Pause without removing
prefect deployment pause dhis2_ou/dhis2-ou
```

See [CLI Reference](cli-reference.md) for the full list of schedule commands.
