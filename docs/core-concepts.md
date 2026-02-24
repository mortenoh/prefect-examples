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
triggers. Deployments are defined in code or YAML and registered with the
Prefect server.

Two deployment methods:

- **`flow.serve()`** — simplest approach, runs in-process. Good for development
  and simple production use.
- **`flow.deploy()`** — sends runs to a work pool for infrastructure-level
  isolation. Requires a running worker.

```python
# Simple: run locally with cron schedule
my_flow.serve(name="my-flow", cron="0 6 * * *")

# Production: deploy to a work pool
my_flow.deploy(name="my-flow", work_pool_name="my-pool")
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

- **CronSchedule** — standard cron expressions (`"0 6 * * *"`)
- **IntervalSchedule** — fixed intervals (`interval=900` seconds)
- **RRuleSchedule** — RFC 5545 recurrence rules (`"FREQ=WEEKLY;BYDAY=MO,WE,FR"`)

Schedules are passed to `flow.serve()` or `flow.deploy()`:

```python
my_flow.serve(name="daily", cron="0 6 * * *")
my_flow.serve(name="every-15m", interval=900)
```

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
