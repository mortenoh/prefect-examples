# Core Concepts

A quick tour of the Prefect 3 building blocks used throughout these examples.

## Flows

A **flow** is the main container for orchestrated work. Decorate any Python
function with `@flow` and Prefect tracks its execution, state, and metadata.

```python
from prefect import flow

@flow(name="my_flow", log_prints=True)
def my_flow():
    print("Running!")
```

Flows can call other flows (subflows), accept typed parameters, and return
values.

## Tasks

A **task** is a unit of work inside a flow. Decorate a function with `@task`
to gain retries, caching, concurrency controls, and observability.

```python
from prefect import task

@task(retries=3, retry_delay_seconds=10)
def extract_data() -> dict:
    ...
```

Tasks are called like normal functions. Dependencies are expressed through
return-value wiring -- pass the output of one task as input to the next.

## States

Every flow run and task run has a **state** that tracks its lifecycle:
`Pending`, `Running`, `Completed`, `Failed`, `Cancelled`, etc.

You can inspect states programmatically:

```python
state = my_flow(return_state=True)
assert state.is_completed()
```

## Results

Tasks and flows return values directly -- no push/pull ceremony required.
In Airflow you would use XCom; in Prefect you simply return and pass values:

```python
@task
def produce() -> dict:
    return {"key": "value"}

@task
def consume(data: dict) -> None:
    print(data["key"])

@flow
def pipeline():
    data = produce()
    consume(data)      # data flows naturally
```

## Deployments

A **deployment** packages a flow for remote execution on a schedule or via API
triggers. A flow is just a Python function; a deployment is the configuration
that says *when*, *where*, and *with what parameters* that function should run.

### Why you need a deployment

Running `python my_flow.py` executes a flow once. A deployment lets you:

- **Schedule** runs (cron, interval, or RRule)
- **Trigger** runs from the UI, API, or CLI
- **Parameterize** runs with different inputs
- **Run on remote infrastructure** (Docker, Kubernetes, etc.)

### `flow.serve()` vs `flow.deploy()`

| | `flow.serve()` | `flow.deploy()` |
|---|---|---|
| Where it runs | Same process | Work pool (separate infra) |
| Infra needed | None | Work pool + worker |
| Scaling | Single process | Pool-level scaling |
| Best for | Dev, simple cron jobs | Production, team use |

```python
# flow.serve() -- simplest approach, runs in-process
my_flow.serve(name="my-flow", cron="0 6 * * *")

# flow.deploy() -- production, sends runs to a work pool
my_flow.deploy(
    name="my-flow",
    work_pool_name="my-pool",
    cron="0 6 * * *",
)
```

Both methods accept `parameters=` to set default parameter values:

```python
my_flow.serve(
    name="dhis2-daily",
    cron="0 6 * * *",
    parameters={"endpoints": ["organisationUnits", "dataElements"]},
)
```

### `prefect.yaml` for declarative deployments

For production, define deployments in a `prefect.yaml` file at the project
root. This is the Prefect equivalent of Airflow's `dags/` folder:

```yaml
deployments:
  - name: dhis2-ou
    entrypoint: flow.py:dhis2_ou_flow
    schedules:
      - cron: "0 6 * * *"
        timezone: "UTC"
    work_pool:
      name: default
```

Deploy with `prefect deploy --all` or `prefect deploy -n dhis2-ou`.

### Work pools

Work pools define *where* flow runs execute. Three common types:

| Pool type | What it does | When to use |
|---|---|---|
| `process` | Runs flows as local subprocesses | Development, single-machine |
| `docker` | Runs flows in Docker containers | Team use, isolation |
| `kubernetes` | Runs flows as K8s jobs | Production, auto-scaling |

Create a pool and start a worker:

```bash
prefect work-pool create my-pool --type process
prefect worker start --pool my-pool
```

### Deployment lifecycle

1. **Create** -- `prefect deploy` or `flow.serve()`/`flow.deploy()`
2. **Schedule** -- runs are created automatically per schedule
3. **Run** -- trigger manually via `prefect deployment run <name>`
4. **Pause** -- `prefect deployment pause <name>` (stops scheduling)
5. **Resume** -- `prefect deployment resume <name>`
6. **Update** -- re-run `prefect deploy` or change schedule via CLI
7. **Delete** -- `prefect deployment delete <name>`

### Prefect CLI for deployments

```bash
# List and inspect
prefect deployment ls                          # list all deployments
prefect deployment inspect <flow/deployment>   # view deployment details

# Trigger runs
prefect deployment run <flow/deployment>                    # run with defaults
prefect deployment run <flow/deployment> -p key=value       # run with params

# Schedule management
prefect deployment set-schedule <name> --cron "0 8 * * *"   # change schedule
prefect deployment set-schedule <name> --interval 3600      # every hour
prefect deployment clear-schedule <name>                    # remove schedule
prefect deployment pause <name>                             # pause scheduling
prefect deployment resume <name>                            # resume scheduling

# Cleanup
prefect deployment delete <name>
```

## Artifacts

**Artifacts** publish rich content (markdown, tables, links) to the Prefect UI.
Use them for reports, dashboards, and reference links.

```python
from prefect.artifacts import create_markdown_artifact, create_table_artifact

create_markdown_artifact(key="report", markdown="# Report\n...")
create_table_artifact(key="data", table=[{"col": "value"}])
```

Without a Prefect server, artifact functions silently no-op.

**See:** [Markdown Artifacts](flow-reference.md#markdown-artifacts),
[Table and Link Artifacts](flow-reference.md#table-and-link-artifacts)

## Tags

**Tags** label flows and tasks for filtering, searching, and grouping in the
Prefect UI. Apply them at definition time or dynamically at runtime:

```python
from prefect import flow, task, tags

@task(tags=["etl", "extract"])
def extract_data() -> dict:
    ...

@flow(name="my_flow", tags=["examples"])
def my_flow() -> None:
    extract_data()

    # Dynamic tags applied at runtime
    with tags("ad-hoc", "debug"):
        debug_task()
```

Tags are additive -- tasks inherit tags from their parent flow, plus any set
via the `tags()` context manager.

**See:** [Tags](flow-reference.md#tags)

## Events

**Events** are custom signals emitted from flows and tasks. Use them to
trigger automations or track domain-specific occurrences:

```python
from prefect.events import emit_event

emit_event(
    event="data.quality.check",
    resource={"prefect.resource.id": "quality-monitor"},
    payload={"score": 0.95, "status": "green"},
)
```

Events are sent to the Prefect event system and can trigger automations
configured in the UI. Without a server, `emit_event()` silently no-ops.

**See:** [Events](flow-reference.md#events)

## Automations

Automations are event-driven rules that pair a **trigger** with an **action**.
When the trigger condition is met (e.g. a flow run fails, an event is emitted,
or a work queue becomes unhealthy), Prefect executes the configured action
automatically. Automations can be created in the UI, via the CLI, or with the
Python SDK.

### Python SDK

Use the `Automation` and `EventTrigger` classes from `prefect.automations`:

```python
from prefect.automations import Automation
from prefect.events.schemas.automations import EventTrigger

# Send a notification when any flow run fails
Automation(
    name="notify-on-failure",
    trigger=EventTrigger(
        expect={"prefect.flow-run.Failed"},
        match={"prefect.resource.id": "prefect.flow-run.*"},
    ),
    actions=[{"type": "send-notification", "block_document_id": "<notification-block-id>"}],
).create()
```

```python
# Circuit breaker: pause a deployment after 3 failures in 10 minutes
from datetime import timedelta

Automation(
    name="circuit-breaker",
    trigger=EventTrigger(
        expect={"prefect.flow-run.Failed"},
        match={"prefect.resource.id": "prefect.flow-run.*"},
        for_each={"prefect.resource.id"},
        threshold=3,
        within=timedelta(minutes=10),
    ),
    actions=[{"type": "pause-deployment"}],
).create()
```

### Common trigger/action patterns

| Trigger | Action | Use case |
|---|---|---|
| Flow run enters `Failed` state | Send notification | Alert on-call engineer |
| N failures within time window | Pause deployment | Circuit breaker |
| Work queue becomes unhealthy | Send notification | Infrastructure monitoring |
| Custom event emitted | Run deployment | Event-driven orchestration |
| Flow run duration exceeds threshold | Cancel flow run | Timeout guard |

### CLI

```bash
prefect automation ls                     # list automations
prefect automation inspect <name>         # view automation details
prefect automation pause <name>           # disable an automation
prefect automation resume <name>          # re-enable an automation
prefect automation delete <name>          # remove an automation
```

### Airflow comparison

| Airflow | Prefect |
|---|---|
| SLA callbacks (`sla_miss_callback`) | Automation with duration trigger |
| PagerDuty / Slack operators in `on_failure_callback` | Notification action on failure trigger |
| Sensors polling for external events | `EventTrigger` reacting to emitted events |
| No built-in circuit breaker | Threshold trigger + pause action |

**See:** [Prefect Automations docs](https://docs.prefect.io/concepts/automations/)

## Custom Run Names

Customise flow run and task run names for easier identification in the UI and
logs. Names support template strings and callable generators:

```python
@task(task_run_name="fetch-{source}-page-{page}")
def fetch_page(source: str, page: int) -> dict:
    ...

@flow(flow_run_name="report-{env}-{date_str}")
def report_flow(env: str, date_str: str) -> None:
    ...
```

For dynamic names, pass a callable:

```python
def generate_name():
    return f"run-{datetime.now():%Y%m%d-%H%M}"

@flow(flow_run_name=generate_name)
def my_flow() -> None:
    ...
```

**See:** [Task Run Names](flow-reference.md#task-run-names),
[Flow Run Names](flow-reference.md#flow-run-names)

## Blocks

**Blocks** are typed, reusable configuration objects. Built-in blocks include
`Secret`, `JSON`, and others. Custom blocks subclass `Block`:

```python
from prefect.blocks.core import Block

class DatabaseConfig(Block):
    host: str = "localhost"
    port: int = 5432

# Use directly or save/load from server
config = DatabaseConfig(host="db.prod.com")
```

The `Secret` block handles encrypted credentials:

```python
from prefect.blocks.system import Secret
api_key = Secret.load("my-key").get()
```

### Block lifecycle

1. **Define** -- subclass `Block` with typed fields
2. **Instantiate** -- create with values: `MyBlock(field="value")`
3. **Save** -- persist to the Prefect server: `block.save("my-block")`
4. **Load** -- retrieve in any flow: `MyBlock.load("my-block")`

Blocks saved to a Prefect server have their `SecretStr` fields encrypted at
rest.  Without a server, blocks work as plain Python objects with inline
defaults.

### When to use Block vs Secret vs JSON

| Type | Use for | Example |
|---|---|---|
| Custom `Block` | Typed connection config with methods | `Dhis2Credentials` |
| `Secret` | Single credential value | API key, token |
| `JSON` | Unstructured config | Feature flags, thresholds |

### SecretStr for credentials

Use Pydantic `SecretStr` for password and token fields on blocks. SecretStr
prevents accidental exposure in logs, repr, and serialization:

```python
from pydantic import Field, SecretStr

class MyConnection(Block):
    username: str = "admin"
    password: SecretStr = Field(default=SecretStr("changeme"))

conn = MyConnection()
conn.password                       # SecretStr('**********')
conn.password.get_secret_value()    # 'changeme'
```

### Adding methods to blocks (the integration pattern)

Official Prefect integrations (prefect-aws, prefect-gcp, prefect-slack) put
a `get_client()` method on credentials blocks that returns an authenticated
SDK client. API methods live on the client class, not the block:

```python
class Dhis2Client:
    """Authenticated DHIS2 API client."""

    def __init__(self, base_url: str, username: str, password: str) -> None:
        self._http = httpx.Client(
            base_url=f"{base_url}/api",
            auth=(username, password),
            timeout=60,
        )

    def fetch_metadata(self, endpoint: str) -> list[dict]:
        resp = self._http.get(f"/{endpoint}", params={"paging": "false"})
        resp.raise_for_status()
        return resp.json()[endpoint]

class Dhis2Credentials(Block):
    base_url: str = "https://play.im.dhis2.org/dev"
    username: str = "admin"
    password: SecretStr = Field(default=SecretStr("district"))

    def get_client(self) -> Dhis2Client:
        return Dhis2Client(
            self.base_url,
            self.username,
            self.password.get_secret_value(),
        )
```

This pattern separates credential storage (Block) from API logic (Client),
following the same convention as `GcpCredentials.get_client()` in prefect-gcp.

### Airflow Connections vs Prefect Blocks

| Airflow | Prefect |
|---|---|
| `Connection` (host, login, password, extras) | Custom `Block` with typed fields |
| `BaseHook.get_connection("conn_id")` | `MyBlock.load("block-name")` |
| Fernet-encrypted in metadata DB | Encrypted at rest in Prefect server |
| Password as plain string | `SecretStr` prevents accidental logging |
| Hook classes with methods | Block classes with methods |

## Pydantic Models

Prefect works natively with **Pydantic models** as task parameters and return
types. This gives automatic validation, serialisation, and type safety:

```python
from pydantic import BaseModel, field_validator

class WeatherReading(BaseModel):
    station_id: str
    temperature: float
    humidity: float

    @field_validator("temperature")
    @classmethod
    def temperature_in_range(cls, v: float) -> float:
        if v < -100 or v > 60:
            raise ValueError(f"Temperature {v} out of range")
        return v
```

Pydantic replaces the manual serialisation required by Airflow's XCom. Models
flow between tasks naturally, with validation happening automatically.

## Transactions

**Transactions** group tasks atomically. If any task in the group fails, the
entire transaction is treated as a unit:

```python
from prefect.transactions import transaction

@flow
def atomic_pipeline():
    with transaction():
        step_a()
        step_b()
        step_c()
```

Transactions are a Prefect-specific feature with no direct Airflow equivalent.

## Async Flows

Prefect natively supports `async def` tasks and flows. Use `asyncio.gather()`
for concurrent I/O-bound work:

```python
@task
async def fetch(url: str) -> dict:
    await asyncio.sleep(0.5)
    return {"url": url}

@flow
async def pipeline() -> None:
    results = await asyncio.gather(fetch("a"), fetch("b"), fetch("c"))
```

Sync and async tasks can be mixed in an async flow. Async flows use
`asyncio.run()` in `__main__`.

## Deployment and Scheduling

Prefect supports three schedule types:

- **CronSchedule** -- standard cron expressions (`"0 6 * * *"`)
- **IntervalSchedule** -- fixed intervals (`interval=900` seconds)
- **RRuleSchedule** -- RFC 5545 recurrence rules (`"FREQ=WEEKLY;BYDAY=MO,WE,FR"`)

Schedules are passed to `flow.serve()` or `flow.deploy()`:

```python
my_flow.serve(name="daily", cron="0 6 * * *")
my_flow.serve(name="every-15m", interval=900)
my_flow.serve(name="weekdays", rrule="FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR")
```

In `prefect.yaml`, schedules are declared per deployment:

```yaml
schedules:
  - cron: "0 6 * * *"
    timezone: "UTC"
```

### Passing parameters to deployments

Deployments can override a flow's default parameters. Parameters are passed
at deployment creation and can be overridden per run:

```python
# At deployment time (default parameters for all runs)
my_flow.serve(
    name="dhis2-sync",
    parameters={"endpoints": ["organisationUnits"]},
)

# Override at run time via CLI
# prefect deployment run my-flow/dhis2-sync -p endpoints='["dataElements"]'
```

### Deployment-aware flows with `prefect.runtime`

Use `prefect.runtime` to access deployment context inside a running flow:

```python
from prefect.runtime import deployment, flow_run

deployment_name = deployment.name            # None for local runs
flow_run_name = flow_run.name                # auto-generated name
scheduled_start = flow_run.scheduled_start_time
```

This lets flows adapt their behaviour depending on whether they are running
locally or inside a deployment.

### Changing schedules after deployment

Use the CLI to update schedules without redeploying:

```bash
prefect deployment set-schedule <name> --cron "0 8 * * *"
prefect deployment set-schedule <name> --interval 1800
prefect deployment set-schedule <name> --rrule "FREQ=DAILY;BYDAY=MO,WE,FR"
prefect deployment clear-schedule <name>
```

Or re-run `prefect deploy` after updating `prefect.yaml`.

## File I/O

Prefect flows handle file I/O with stdlib modules (`csv`, `json`, `pathlib`,
`tempfile`). Each file operation is a `@task` for observability:

```python
@task
def read_csv(path: Path) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))
```

Use `tempfile.mkdtemp()` for isolated working directories in flows, and
`tmp_path` in tests. For mixed file types, dispatch on path suffix. Track
processed files in a JSON manifest for incremental processing.

## Data Quality

Define quality rules as configuration and execute them against data:

```python
@task
def execute_rule(data: list[dict], rule: QualityRule) -> RuleResult:
    if rule.rule_type == "not_null":
        return run_not_null_check.fn(data, rule.column)
```

Score rules individually, then compute an overall quality score with
traffic-light classification (green/amber/red). For cross-dataset validation,
check referential integrity between related datasets.

Statistical profiling uses the stdlib `statistics` module (mean, stdev, median)
to profile columns by inferred type (numeric vs string).

## Analytics and Modeling

Phase 5 introduces statistical analysis and modeling patterns:

- **Pearson correlation** -- manual implementation using `math` and `statistics`
  modules (no numpy/scipy required)
- **Linear regression** -- ordinary least squares with R-squared computation
- **Star schema** -- dimensional modeling with fact and dimension tables,
  surrogate keys, and composite index ranking
- **Log returns** -- financial time series analysis with rolling volatility
- **Hypothesis testing** -- educational null hypothesis validation pattern

These patterns demonstrate that analytics pipelines can be built with
stdlib modules alone, making flows lightweight and dependency-free.

## Blocks and Connections

In Airflow, external system credentials are stored as **Connections** and
accessed via `BaseHook.get_connection("conn_id")`. In Prefect, the equivalent
is a custom **Block** -- a typed, serializable configuration object whose
`get_client()` method returns an authenticated API client:

```python
from prefect.blocks.core import Block
from pydantic import Field, SecretStr

class Dhis2Credentials(Block):
    base_url: str = "https://play.im.dhis2.org/dev"
    username: str = "admin"
    password: SecretStr = Field(default=SecretStr("district"))

    def get_client(self) -> Dhis2Client:
        return Dhis2Client(
            self.base_url,
            self.username,
            self.password.get_secret_value(),
        )

# Register once:
Dhis2Credentials(base_url="https://dhis2.example.org").save("dhis2")

# Load in any flow:
creds = Dhis2Credentials.load("dhis2")
client = creds.get_client()
units = client.fetch_metadata("organisationUnits")
```

Password is stored as `SecretStr` directly on the block. When saved to a
Prefect server, `SecretStr` fields are encrypted at rest.

Multiple configuration strategies can coexist -- inline defaults for
development, saved blocks for production, environment variables for CI:

| Strategy | Best for | Example |
|---|---|---|
| Inline `Block()` | Development, testing | `Dhis2Credentials()` |
| `Block.load()` | Production with Prefect server | `Dhis2Credentials.load("dhis2")` |
| `SecretStr` on Block | Credentials with config | `password: SecretStr` |
| `Secret.load()` | Standalone passwords, API keys | `Secret.load("dhis2-password")` |
| `os.environ` | CI/CD, containers | `os.environ["DHIS2_PASSWORD"]` |
| `JSON.load()` | Structured config | `JSON.load("dhis2-config")` |

## Airflow to Prefect comparison

| Airflow concept | Prefect equivalent | Example |
|---|---|---|
| DAG | `@flow` | 001 |
| PythonOperator | `@task` | 002 |
| `>>` / `set_downstream` | Return-value wiring, `.submit()` | 003 |
| TaskFlow API (`@task`) | Native -- Prefect is taskflow-first | 004 |
| XCom push/pull | Return values | 005 |
| BranchPythonOperator | Python `if/elif/else` | 006 |
| `on_failure_callback` / trigger_rule | State hooks, `allow_failure` | 007 |
| Jinja2 templating / params | Typed function parameters | 008 |
| TaskGroup / SubDagOperator | Subflows (`@flow` calling `@flow`) | 009 |
| `expand()` (dynamic task mapping) | `.map()` | 010 |
| Sensor (poke/reschedule) | While-loop polling | 011 |
| `retries` + callbacks | `retries`, `retry_delay_seconds`, hooks | 012 |
| Custom operators / shared utils | Python imports | 013 |
| Custom XCom + trigger rules | `emit_event()` | 014 |
| TriggerDagRunOperator | Subflow calls, `run_deployment()` | 015 |
| Pool slots | `concurrency()` context manager | 016 |
| Variables + params | `Variable.get()`/`set()` | 017 |
| ShortCircuitOperator | Python `return` | 018 |
| `@setup` / `@teardown` | Context managers, `try/finally` | 019 |
| Complex DAG | Subflows + `.map()` + hooks | 020 |
| Custom caching / Redis | `cache_policy`, `cache_key_fn` | 021 |
| `execution_timeout` | `timeout_seconds` | 022 |
| Custom `task_id` | `task_run_name` | 023 |
| `exponential_backoff` | `retry_delay_seconds` list, `retry_jitter_factor` | 024 |
| Task instance logger | `get_run_logger()` | 025 |
| DAG/task tags | `tags=`, `tags()` context manager | 026 |
| Custom `run_id` | `flow_run_name` | 027 |
| XCom backend config | `persist_result`, `result_storage_key` | 028 |
| Custom HTML / reports | `create_markdown_artifact()` | 029 |
| UI plugins | `create_table_artifact()`, `create_link_artifact()` | 030 |
| Connections (encrypted) | `Secret` block | 031 |
| Custom connection types | Custom `Block` subclass | 032 |
| Deferrable operators | `async def` tasks and flows | 033 |
| Parallel deferrable ops | `asyncio.gather()` | 034 |
| Mixed operator types | Sync + async tasks in async flow | 035 |
| Dynamic mapping + async | `.map()` / `.submit()` with async tasks | 036 |
| DAG in `dags/` folder | `flow.serve()` | 037 |
| `schedule_interval` | `CronSchedule`, `IntervalSchedule`, `RRuleSchedule` | 038 |
| Executors (Celery, K8s) | Work pools + workers | 039 |
| Production DAG | Caching + retries + artifacts + tags | 040 |
| XCom + complex types | Pydantic `BaseModel` params/returns | 041 |
| BashOperator | `subprocess.run()` in a `@task` | 042 |
| HttpOperator | `httpx` in a `@task` | 043 |
| Custom operators | Task factory functions | 044 |
| `expand_kwargs` | Multi-arg `.map()` | 045 |
| Error handling patterns | Quarantine pattern with Pydantic | 046 |
| Schema validation | Pydantic `field_validator` | 047 |
| SLA miss detection | `time.monotonic()` + threshold checks | 048 |
| Webhook callbacks | `httpx.post()` + flow hooks | 049 |
| Progressive retry | `retries` + `on_failure` hooks | 050 |
| Thin DAG wiring | Pure functions + thin `@task` wrappers | 051 |
| Custom hooks/sensors | Python decorators wrapping `@task` | 052 |
| Trigger rules | `allow_failure`, state inspection | 053 |
| TaskGroups | Nested subflows (`@flow` calling `@flow`) | 054 |
| Backfill / `logical_date` | Flow parameters for date ranges | 055 |
| Jinja `{{ ds }}` macros | `prefect.runtime` context | 056 |
| No equivalent | `transaction()` for atomic groups | 057 |
| Human-in-the-loop ops | `pause_flow_run()` / approval pattern | 058 |
| Executors | Task runners (`ThreadPoolTaskRunner`) | 059 |
| Full ETL SCD pipeline | Capstone: all Phase 3 features | 060 |
| CSV landing zone | stdlib `csv` in `@task` | 061 |
| JSON event ingestion | Recursive flatten, NDJSON output | 062 |
| Multi-file batch | File-type dispatch, hash dedup | 063 |
| Incremental file processing | JSON manifest tracking | 064 |
| Freshness/completeness checks | Config-driven quality rules | 065 |
| Referential integrity | FK checks between datasets | 066 |
| Quality dashboard | Statistical profiling (`statistics`) | 067 |
| Pipeline health check | Meta-monitoring / watchdog | 068 |
| Multi-city forecast | Chained `.map()` calls | 069 |
| Paginated API fetch | Offset/limit simulation, chunked `.map()` | 070 |
| Cross-API enrichment | Multi-source join, partial fallback | 071 |
| Cached API comparison | Application-level cache with TTL | 072 |
| API-triggered config | Config-driven stage dispatch | 073 |
| Asset producer/consumer | File-based data contracts | 074 |
| No equivalent | Circuit breaker state machine | 075 |
| Multi-API dashboard | Pydantic discriminated unions | 076 |
| GeoJSON / OData pivot | Windowed batch, anomaly detection | 077 |
| No equivalent | Hash-based idempotency registry | 078 |
| No equivalent | Checkpoint-based stage recovery | 079 |
| Quality framework + dashboard | Capstone: all Phase 4 features | 080 |
| WHO threshold classification | Threshold-based AQI classification | 081 |
| Weighted risk scoring | Multi-source composite risk index | 082 |
| Seasonal analysis | Latitude-daylight correlation | 083 |
| Parquet aggregation | Fan-out grouped aggregation | 084 |
| Nested JSON normalization | Pydantic model flattening | 085 |
| Multi-indicator correlation | Pearson correlation matrix | 086 |
| Currency volatility analysis | Log returns, rolling volatility | 087 |
| Cross-domain hypothesis test | Null hypothesis validation | 088 |
| Log-linear regression | Manual OLS regression | 089 |
| Dimensional modeling | Star schema, composite index | 090 |
| SQL-based ETL layers | Simulated staging/production/summary | 091 |
| Generic data transfer | Category computation, checksum verification | 092 |
| Org unit hierarchy | Tree flattening, path-based depth | 093 |
| Expression parsing | Regex complexity scoring | 094 |
| GeoJSON construction | Spatial feature collection | 095 |
| Combined parallel export | Fan-in multi-endpoint summary | 096 |
| No equivalent | Data lineage tracking (hashlib) | 097 |
| No equivalent | Pipeline template factory | 098 |
| No equivalent | Multi-pipeline orchestrator | 099 |
| Full analytics pipeline | Grand capstone: all Phase 5 patterns | 100 |
| `BaseHook.get_connection()` | Custom `Block` with methods + `SecretStr` | 101 |
| DHIS2 org unit fetch | Block auth + Pydantic flattening | 102 |
| DHIS2 data element fetch | Block auth + categorization | 103 |
| DHIS2 indicator fetch | Block auth + regex expression parsing | 104 |
| DHIS2 geometry export | Block auth + GeoJSON construction | 105 |
| DHIS2 combined export | Parallel `.submit()` + shared block | 106 |
| DHIS2 analytics query | Dimension query + headers/rows parsing | 107 |
| Full DHIS2 pipeline | Multi-stage pipeline + quality + dashboard | 108 |
| Connection/Variable config | Multiple config strategies (Block, Secret, env) | 109 |
| Authenticated API pattern | Pluggable auth block (api_key, bearer, basic) | 110 |
| Scheduled DAG + Connections | `flow.deploy()` with blocks + artifacts | `deployments/dhis2_ou` |
