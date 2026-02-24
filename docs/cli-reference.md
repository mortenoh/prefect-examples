# CLI Reference

Prefect CLI commands used in this project, organised by workflow. All commands
assume the virtual environment is active (prefix with `uv run` if needed).

---

## Server management

Start a local Prefect server with the UI at `http://localhost:4200`:

```bash
prefect server start
```

Useful environment variables:

| Variable | Description | Default |
|---|---|---|
| `PREFECT_API_URL` | API endpoint the client talks to | `http://127.0.0.1:4200/api` |
| `PREFECT_SERVER_ANALYTICS_ENABLED` | Send anonymous usage stats | `true` |
| `PREFECT_SERVER_UI_SHOW_PROMOTIONAL_CONTENT` | Show promotional banners in UI | `true` |
| `PREFECT_API_DATABASE_CONNECTION_URL` | Database connection string | SQLite (default) |

For a full stack with PostgreSQL, see [Infrastructure](infrastructure.md).

---

## Deployments

### Creating deployments

Deploy all flows defined in a `prefect.yaml`:

```bash
prefect deploy --all
```

Deploy a single named deployment:

```bash
prefect deploy -n dhis2-ou
```

### Listing and inspecting

```bash
prefect deployment ls                           # list all deployments
prefect deployment inspect <flow/deployment>    # view deployment details
```

### Triggering runs

```bash
# Run with default parameters
prefect deployment run <flow/deployment>

# Run with parameter overrides
prefect deployment run <flow/deployment> -p key=value
```

### Schedule management

Prefect supports three schedule types -- cron, interval, and RRule. Schedules
can be set at deployment time in `prefect.yaml` or updated after deployment
via the CLI.

#### Setting schedules in `prefect.yaml`

Schedules are declared per deployment. Multiple schedules per deployment are
supported:

```yaml
deployments:
  - name: dhis2-ou
    entrypoint: flow.py:dhis2_ou_flow
    schedules:
      - cron: "0 6 * * *"        # daily at 06:00 UTC
        timezone: "UTC"
      - cron: "0 18 * * *"       # also at 18:00 UTC
        timezone: "UTC"
    work_pool:
      name: default
```

**Cron expressions:**

| Expression | Meaning |
|---|---|
| `"0 6 * * *"` | Daily at 06:00 |
| `"*/15 * * * *"` | Every 15 minutes |
| `"0 8 * * 1-5"` | Weekdays at 08:00 |
| `"0 0 1 * *"` | First day of each month at midnight |

**Interval and RRule in code:**

```python
# Interval -- every 15 minutes
my_flow.serve(name="frequent", interval=900)

# RRule -- weekdays only
my_flow.serve(name="weekdays", rrule="FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR")
```

#### Updating schedules after deployment

Change schedules without redeploying:

```bash
# Set a cron schedule
prefect deployment set-schedule <flow/deployment> --cron "0 8 * * *"

# Set an interval schedule (seconds)
prefect deployment set-schedule <flow/deployment> --interval 3600

# Set an RRule schedule
prefect deployment set-schedule <flow/deployment> --rrule "FREQ=WEEKLY;BYDAY=MO,WE,FR"

# Remove all schedules
prefect deployment clear-schedule <flow/deployment>
```

#### Pausing and resuming schedules

```bash
# Pause scheduling (existing runs are not affected)
prefect deployment pause <flow/deployment>

# Resume scheduling
prefect deployment resume <flow/deployment>
```

#### Viewing schedules

```bash
# Inspect shows schedule details
prefect deployment inspect <flow/deployment>
```

The Prefect UI (`http://localhost:4200`) also shows schedules on the deployment
detail page, where they can be edited visually.

### Passing parameters to deployments

Default parameters are set in `prefect.yaml`:

```yaml
deployments:
  - name: dhis2-sync
    entrypoint: flow.py:dhis2_sync_flow
    parameters:
      endpoints:
        - organisationUnits
        - dataElements
    schedules:
      - cron: "0 6 * * *"
        timezone: "UTC"
    work_pool:
      name: default
```

Override at run time:

```bash
prefect deployment run dhis2_sync/dhis2-sync -p endpoints='["indicators"]'
```

Parameters can also be overridden from the Prefect UI when triggering a
manual run.

### Deployment lifecycle

| Action | Command |
|---|---|
| Create / update | `prefect deploy --all` or `prefect deploy -n <name>` |
| List | `prefect deployment ls` |
| Inspect | `prefect deployment inspect <flow/deployment>` |
| Trigger run | `prefect deployment run <flow/deployment>` |
| Set schedule | `prefect deployment set-schedule <name> --cron "..."` |
| Clear schedule | `prefect deployment clear-schedule <name>` |
| Pause | `prefect deployment pause <name>` |
| Resume | `prefect deployment resume <name>` |
| Delete | `prefect deployment delete <name>` |

---

## Work pools

### Creating a work pool

```bash
prefect work-pool create my-pool --type process
```

Pool types:

| Type | Description |
|---|---|
| `process` | Runs flows as local subprocesses (development) |
| `docker` | Runs flows in Docker containers (team use) |
| `kubernetes` | Runs flows as K8s jobs (production) |

### Starting a worker

```bash
prefect worker start --pool my-pool
```

The worker polls the pool for scheduled runs and executes them. Multiple
workers can share the same pool for horizontal scaling.

### Managing work pools

```bash
prefect work-pool ls                    # list all pools
prefect work-pool inspect my-pool       # view pool details
prefect work-pool pause my-pool         # pause the pool
prefect work-pool resume my-pool        # resume the pool
prefect work-pool delete my-pool        # delete the pool
```

---

## Blocks

### Registering block types

```bash
prefect block register -m prefect_examples.dhis2
```

This registers custom block types (like `Dhis2Credentials`) with the Prefect
server so they appear in the UI.

### Managing blocks

```bash
prefect block ls                        # list all saved blocks
prefect block inspect <type/name>       # view block details
prefect block delete <type/name>        # delete a block
```

---

## Flow runs

```bash
prefect flow-run ls                     # list recent flow runs
prefect flow-run inspect <id>           # view run details
prefect flow-run cancel <id>            # cancel a running flow
prefect flow-run delete <id>            # delete a flow run
```

---

## Automations

```bash
prefect automation ls                     # list automations
prefect automation inspect <name>         # view automation details
prefect automation delete <name>          # remove an automation
prefect automation pause <name>           # disable an automation
prefect automation resume <name>          # re-enable an automation
```

---

## Makefile shortcuts

The project `Makefile` wraps common commands:

| Target | Command | Description |
|---|---|---|
| `make help` | -- | Show all available targets |
| `make sync` | `uv sync` | Install dependencies |
| `make lint` | `uv run ruff check . && uv run mypy src/ packages/` | Run linter and type checker |
| `make fmt` | `uv run ruff format . && uv run ruff check --fix .` | Auto-format code |
| `make test` | `uv run pytest` | Run the test suite |
| `make clean` | `rm -rf ...` | Remove build artifacts |
| `make run` | `uv run python flows/001_hello_world.py` | Run the hello-world flow |
| `make server` | `uv run prefect server start` | Start a local Prefect server |
| `make start` | `docker compose up --build` | Start the full Docker stack |
| `make restart` | `docker compose down -v && ... up` | Tear down and rebuild the stack |
| `make deploy` | `prefect deploy --all` (per deployment dir) | Register all deployments |
| `make docs` | `uv run mkdocs serve` | Serve docs locally |
| `make register-blocks` | `prefect block register -m prefect_dhis2` | Register custom block types with the server |
| `make docs-build` | `uv run mkdocs build` | Build static docs site |
