# Flow Reference

Detailed walkthrough of all 60 example flows, organised by category.

---

## Basics (001--005)

### 001 -- Hello World

**What it demonstrates:** The simplest possible Prefect flow -- two tasks
executed sequentially.

**Airflow equivalent:** BashOperator tasks with `>>` dependency.

```python
@task
def say_hello() -> str:
    msg = "Hello from Prefect!"
    print(msg)
    return msg

@flow(name="001_hello_world", log_prints=True)
def hello_world() -> None:
    say_hello()
    print_date()
```

Tasks are plain Python functions. Call them inside a `@flow` and Prefect tracks
everything automatically.

---

### 002 -- Python Tasks

**What it demonstrates:** Tasks with typed parameters and return values.

**Airflow equivalent:** PythonOperator with `python_callable`.

```python
@task
def greet(name: str, greeting: str = "Hello") -> str:
    msg = f"{greeting}, {name}!"
    print(msg)
    return msg

@task
def compute_sum(a: int, b: int) -> int:
    result = a + b
    print(f"{a} + {b} = {result}")
    return result
```

Any Python function becomes a task with `@task` -- type hints, defaults, and
docstrings all work as expected.

---

### 003 -- Task Dependencies

**What it demonstrates:** Parallel fan-out with `.submit()` and a join step.

**Airflow equivalent:** `>>` operator / `set_downstream`.

```python
@flow(name="003_task_dependencies", log_prints=True)
def task_dependencies_flow() -> None:
    initial = start()

    future_a = task_a.submit(initial)
    future_b = task_b.submit(initial)
    future_c = task_c.submit(initial)

    join([future_a.result(), future_b.result(), future_c.result()])
```

`.submit()` launches tasks concurrently. Call `.result()` to wait for their
outputs before passing them downstream.

---

### 004 -- Taskflow ETL

**What it demonstrates:** Classic extract-transform-load wired through return
values.

**Airflow equivalent:** TaskFlow API (`@task`).

```python
@flow(name="004_taskflow_etl", log_prints=True)
def taskflow_etl_flow() -> None:
    raw = extract()
    transformed = transform(raw)
    load(transformed)
```

Prefect is natively taskflow-first. Each task returns data and the next task
receives it -- no XCom needed.

---

### 005 -- Task Results

**What it demonstrates:** Passing structured data (dicts, lists) between tasks.

**Airflow equivalent:** XCom push/pull.

```python
@task
def produce_metrics() -> dict[str, Any]:
    return {"total": 150, "average": 37.5, "items": ["alpha", "beta", "gamma", "delta"]}

@task
def consume_metrics(metrics: dict[str, Any]) -> str:
    summary = f"Total: {metrics['total']}, Average: {metrics['average']}"
    return summary
```

Return values replace XCom entirely. Pass dicts, lists, or any serialisable
object between tasks.

---

## Control Flow (006--008)

### 006 -- Conditional Logic

**What it demonstrates:** Branching with plain Python `if/elif/else`.

**Airflow equivalent:** BranchPythonOperator.

```python
@flow(name="006_conditional_logic", log_prints=True)
def conditional_logic_flow() -> None:
    branch = check_condition()

    if branch == "a":
        path_a()
    elif branch == "b":
        path_b()
    else:
        default_path()
```

No special operators needed. Python control flow works directly inside flows.

---

### 007 -- State Handlers

**What it demonstrates:** Reacting to task/flow state changes with hook
functions, and continuing past failures with `allow_failure`.

**Airflow equivalent:** `on_failure_callback` / trigger_rule.

```python
@task(on_failure=[on_task_failure])
def fail_task():
    raise ValueError("Intentional failure for demonstration")

@flow(name="007_state_handlers", log_prints=True, on_completion=[on_flow_completion])
def state_handlers_flow() -> None:
    succeed_task()
    failing_future = fail_task.submit()
    always_run_task(wait_for=[allow_failure(failing_future)])
```

Hooks are plain functions (not tasks) that receive `task`, `task_run`, and
`state`. `allow_failure` lets downstream tasks run even when upstream tasks
fail.

---

### 008 -- Parameterized Flows

**What it demonstrates:** Runtime parameters with typed defaults.

**Airflow equivalent:** Jinja2 templating / params dict.

```python
@flow(name="008_parameterized_flows", log_prints=True)
def parameterized_flow(
    name: str = "World",
    date_str: str | None = None,
    template: str = "Greetings, {name}! Today is {date}.",
) -> None:
    if date_str is None:
        date_str = datetime.date.today().isoformat()
    build_greeting(name, date_str, template)
```

Flow parameters are regular Python function arguments. Type hints and defaults
are preserved in the Prefect UI when the flow is deployed.

---

## Composition (009--010)

### 009 -- Subflows

**What it demonstrates:** Composing larger pipelines from smaller, reusable
flows.

**Airflow equivalent:** TaskGroup / SubDagOperator.

```python
@flow(name="009_subflows", log_prints=True)
def pipeline_flow() -> None:
    raw = extract_flow()
    transformed = transform_flow(raw)
    load_flow(transformed)
```

A `@flow` can call other `@flow` functions. Each subflow appears as a nested
flow run in the Prefect UI with its own state tracking.

---

### 010 -- Dynamic Tasks

**What it demonstrates:** Dynamic fan-out over a list of items with `.map()`.

**Airflow equivalent:** Dynamic task mapping (`expand()`).

```python
@flow(name="010_dynamic_tasks", log_prints=True)
def dynamic_tasks_flow() -> None:
    items = generate_items()
    processed = process_item.map(items)
    summarize(processed)
```

`.map()` creates one task run per item. The number of items can vary at
runtime -- no DAG rewrite required.

---

## Operational (011--012)

### 011 -- Polling Tasks

**What it demonstrates:** Waiting for an external condition with a polling
loop.

**Airflow equivalent:** Sensor (poke/reschedule).

```python
@task
def poll_condition(name: str, interval: float = 1.0, timeout: float = 10.0,
                   succeed_after: float = 3.0) -> str:
    start_time = time.monotonic()
    while True:
        elapsed = time.monotonic() - start_time
        if elapsed >= succeed_after:
            return f"[{name}] Condition met after {elapsed:.1f}s"
        if elapsed >= timeout:
            raise TimeoutError(f"[{name}] Timed out after {elapsed:.1f}s")
        time.sleep(interval)
```

No special sensor class needed. A `while` loop with `time.sleep()` inside a
task accomplishes the same thing.

---

### 012 -- Retries and Hooks

**What it demonstrates:** Automatic retries and lifecycle hooks on tasks and
flows.

**Airflow equivalent:** `retries` + `on_failure_callback`.

```python
@task(retries=3, retry_delay_seconds=1, on_failure=[my_task_failure_hook])
def flaky_task(fail_count: int = 2) -> str:
    key = "flaky_task"
    _attempt_counter[key] = _attempt_counter.get(key, 0) + 1
    attempt = _attempt_counter[key]
    if attempt <= fail_count:
        raise ValueError(f"Attempt {attempt}/{fail_count} — simulated failure")
    return f"flaky_task succeeded on attempt {attempt}"
```

`retries` and `retry_delay_seconds` are set on the decorator. Hooks fire on
state transitions for logging or alerting.

---

## Reuse and Events (013--014)

### 013 -- Reusable Tasks

**What it demonstrates:** Importing shared tasks from a project task library.

**Airflow equivalent:** Custom operators / shared utils.

```python
from prefect_examples.tasks import print_message, square_number

@flow(name="013_reusable_tasks", log_prints=True)
def reusable_tasks_flow() -> None:
    print_message("Hello from reusable tasks!")
    result = square_number(7)
```

Tasks are just Python functions. Import them from a shared module and call them
in any flow. The shared library lives in `src/prefect_examples/tasks.py`.

---

### 014 -- Events

**What it demonstrates:** Emitting custom Prefect events for observability and
automation triggers.

**Airflow equivalent:** Custom XCom + trigger rules.

```python
@task
def emit_completion_event(result: str) -> None:
    emit_event(
        event="flow.data.produced",
        resource={"prefect.resource.id": "prefect_examples.014"},
        payload={"result": result},
    )
```

`emit_event()` sends custom events to the Prefect event system. These can
trigger automations, dashboards, or downstream workflows.

---

## Advanced (015--020)

### 015 -- Flow of Flows

**What it demonstrates:** Orchestrating multiple flows from a parent flow.

**Airflow equivalent:** TriggerDagRunOperator.

```python
@flow(name="015_flow_of_flows", log_prints=True)
def orchestrator() -> None:
    raw = ingest_flow()
    processed = transform_flow(raw)
    summary = report_flow(processed)
    print(f"Pipeline complete: {summary}")
```

The orchestrator calls subflows (ingest, transform, report) in sequence. Each
subflow is independently testable and reusable. For deployed flows, use
`run_deployment()` to trigger remote execution.

---

### 016 -- Concurrency Limits

**What it demonstrates:** Throttling parallel task execution with named limits.

**Airflow equivalent:** Pool slots.

```python
@task
def limited_task(item: str) -> str:
    with concurrency("demo-limit", occupy=1):
        print(f"Processing {item!r} ...")
        time.sleep(0.5)
    return f"processed:{item}"
```

The `concurrency()` context manager from `prefect.concurrency.sync` limits how
many tasks can enter a critical section simultaneously. The limit name
(`"demo-limit"`) is shared across all task runs.

---

### 017 -- Variables and Params

**What it demonstrates:** Storing and retrieving runtime configuration.

**Airflow equivalent:** Variables + params.

```python
@task
def read_config() -> dict:
    Variable.set("example_config", '{"debug": true, "batch_size": 100}', overwrite=True)
    raw = Variable.get("example_config", default="{}")
    config = json.loads(raw)
    return config
```

`Variable.get()` and `Variable.set()` store key-value pairs in the Prefect
backend. Combine with typed flow parameters for full runtime configuration.

---

### 018 -- Early Return

**What it demonstrates:** Short-circuiting a flow with a plain `return`.

**Airflow equivalent:** ShortCircuitOperator.

```python
@flow(name="018_early_return", log_prints=True)
def early_return_flow(skip: bool = False) -> None:
    if skip:
        print("Skip flag is set — returning early")
        return

    proceed = should_continue()
    if not proceed:
        return

    do_work()
    do_more_work()
```

No special operator. A Python `return` statement exits the flow early and
marks it as `Completed`.

---

### 019 -- Context Managers

**What it demonstrates:** Resource setup and teardown with `try/finally`.

**Airflow equivalent:** `@setup` / `@teardown` decorators.

```python
@flow(name="019_context_managers", log_prints=True)
def context_managers_flow() -> None:
    resource = setup_resource()
    try:
        use_resource(resource)
    finally:
        cleanup_resource(resource)
```

Standard Python resource management patterns (context managers, `try/finally`)
work inside flows and guarantee teardown even on failure.

---

### 020 -- Complex Pipeline

**What it demonstrates:** End-to-end pipeline combining subflows, mapped tasks,
and notifications.

**Airflow equivalent:** Complex DAG with branching, sensors, callbacks.

```python
@flow(name="020_complex_pipeline", log_prints=True)
def complex_pipeline() -> None:
    raw = extract_stage()
    transformed = transform_stage(raw)
    summary = load_stage(transformed)
    notify(summary)
```

The transform stage uses chained `.map()` calls:

```python
@flow(name="020_transform", log_prints=True)
def transform_stage(raw: list[dict]) -> list[dict]:
    validated = validate_record.map(raw)
    enriched = enrich_record.map(validated)
    return [future.result() for future in enriched]
```

This is the capstone flow for Phase 1, demonstrating how subflows, mapped tasks,
result passing, and post-pipeline notifications compose into a realistic data
pipeline.

---

## Task-Level Configuration (021--024)

### 021 -- Task Caching

**What it demonstrates:** Task-level caching to avoid redundant computation.

**Airflow equivalent:** Custom caching logic or external cache (Redis, etc.).

```python
from prefect.cache_policies import INPUTS, TASK_SOURCE

@task(cache_policy=INPUTS, cache_expiration=300)
def expensive_computation(x: int, y: int) -> int:
    return x * y

@task(cache_policy=TASK_SOURCE + INPUTS)
def compound_cache_task(data: str) -> str:
    return data.upper()

@task(cache_key_fn=_category_cache_key, cache_expiration=600)
def cached_lookup(category: str, item_id: int) -> dict:
    return {"category": category, "item_id": item_id}
```

Three caching strategies: `INPUTS` (cache by arguments), `TASK_SOURCE + INPUTS`
(invalidate when code or args change), and `cache_key_fn` for custom cache keys.
Cache hits are only visible in Prefect runtime.

---

### 022 -- Task Timeouts

**What it demonstrates:** Task-level and flow-level timeout configuration.

**Airflow equivalent:** `execution_timeout` on operators.

```python
@task(timeout_seconds=3)
def quick_task() -> str:
    return "completed in time"

@task(timeout_seconds=2)
def slow_task() -> str:
    time.sleep(10)  # Will be interrupted by timeout
    return "completed"

@flow(name="022_task_timeouts", log_prints=True, timeout_seconds=30)
def task_timeouts_flow() -> None:
    quick_task()
    try:
        slow_task()
    except Exception:
        cleanup_task(timed_out=True)
```

`timeout_seconds` on `@task` or `@flow` kills execution that exceeds the limit.
The flow catches the timeout and runs cleanup. Note: `.fn()` bypasses timeouts.

---

### 023 -- Task Run Names

**What it demonstrates:** Custom task run naming using templates and callables.

**Airflow equivalent:** `task_id` / custom logging for operator identification.

```python
@task(task_run_name="fetch-{source}-page-{page}")
def fetch_data(source: str, page: int) -> dict:
    return {"source": source, "page": page, "records": page * 10}

def generate_task_name() -> str:
    params = task_run.parameters
    return f"process-{params['region']}-batch-{params['batch_id']}"

@task(task_run_name=generate_task_name)
def process_batch(region: str, batch_id: int) -> str:
    return f"Processed batch {batch_id} for region {region}"
```

Template strings use parameter names in braces. Callables access
`prefect.runtime.task_run.parameters` for dynamic naming.

---

### 024 -- Advanced Retries

**What it demonstrates:** Advanced retry configuration: backoff, jitter, and
conditional retry logic.

**Airflow equivalent:** Custom retry logic, `exponential_backoff`.

```python
@task(retries=3, retry_delay_seconds=[1, 2, 4])
def backoff_task(fail_count: int = 2) -> str: ...

@task(retries=2, retry_delay_seconds=1, retry_jitter_factor=0.5)
def jittery_task(fail_count: int = 1) -> str: ...

def retry_on_value_error(task, task_run, state) -> bool:
    return isinstance(state.result(raise_on_failure=False), ValueError)

@task(retries=2, retry_condition_fn=retry_on_value_error)
def conditional_retry_task(error_type: str) -> str: ...
```

`retry_delay_seconds` accepts a list for escalating delays. `retry_jitter_factor`
adds randomness to prevent thundering herd. `retry_condition_fn` controls which
errors trigger retries.

---

## Flow-Level Configuration (025--028)

### 025 -- Structured Logging

**What it demonstrates:** Prefect's structured logging with `get_run_logger()`,
print capture, and extra context fields.

**Airflow equivalent:** Python logging in operators, task instance logger.

```python
from prefect import get_run_logger

@task
def task_with_logger(item: str) -> str:
    logger = get_run_logger()
    logger.info("Processing %s", item)
    return f"processed:{item}"

@task
def task_with_extra_context(user: str, action: str) -> str:
    logger = get_run_logger()
    logger.info("Action performed", extra={"user": user, "action": action})
    return f"User {user} performed {action}"
```

`get_run_logger()` returns a logger bound to the current run. Outside Prefect
runtime it falls back to stdlib logging. With `log_prints=True`, `print()`
output is captured as INFO-level log entries.

---

### 026 -- Tags

**What it demonstrates:** Tagging tasks and flows for organisation and filtering.

**Airflow equivalent:** DAG/task tags for filtering in the UI.

```python
from prefect import flow, tags, task

@task(tags=["etl", "extract"])
def extract_sales() -> list[dict]: ...

@flow(name="026_tags", log_prints=True, tags=["examples", "phase-2"])
def tags_flow() -> None:
    extract_sales()
    with tags("ad-hoc", "debug"):
        generic_task("debug-data")
```

Static tags are set on decorators. The `tags()` context manager adds runtime
tags to all tasks within its scope. Tags are visible in the Prefect UI and can
be used for filtering and automation rules.

---

### 027 -- Flow Run Names

**What it demonstrates:** Custom flow run naming using templates and callables.

**Airflow equivalent:** Custom DAG `run_id` / `dag_run` naming.

```python
@flow(flow_run_name="report-{env}-{date_str}", log_prints=True)
def template_named_flow(env: str, date_str: str) -> str: ...

def generate_flow_name() -> str:
    ts = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d-%H%M%S")
    return f"dynamic-{ts}"

@flow(flow_run_name=generate_flow_name, log_prints=True)
def callable_named_flow() -> str: ...
```

Works like task run names but on `@flow`. Template strings and callables are
both supported.

---

### 028 -- Result Persistence

**What it demonstrates:** Persisting task and flow results for durability.

**Airflow equivalent:** XCom backend configuration, custom result backends.

```python
@task(persist_result=True)
def compute_metrics(data: list[int]) -> dict:
    return {"total": sum(data), "mean": statistics.mean(data)}

@task(persist_result=True, result_storage_key="latest-summary-{parameters[label]}")
def build_summary(metrics: dict, label: str) -> str:
    return f"[{label}] Total: {metrics['total']}"
```

`persist_result=True` stores results beyond the flow run lifetime.
`result_storage_key` provides a stable key for retrieval. Persistence behaviour
requires a Prefect server; tests verify logic only.

---

## Artifacts and Blocks (029--032)

### 029 -- Markdown Artifacts

**What it demonstrates:** Creating markdown artifacts for rich reporting.

**Airflow equivalent:** Custom HTML in XCom or external reporting tools.

```python
from prefect.artifacts import create_markdown_artifact

@task
def publish_report(results: list[dict]) -> str:
    markdown = "# Report\n| Name | Score |\n|---|---|\n"
    markdown += "\n".join(f"| {r['name']} | {r['score']} |" for r in results)
    create_markdown_artifact(key="report", markdown=markdown, description="Weekly report")
    return markdown
```

`create_markdown_artifact()` publishes formatted content visible in the Prefect
UI. Without a server, it silently no-ops — tests pass locally.

---

### 030 -- Table and Link Artifacts

**What it demonstrates:** Table and link artifacts for structured data display.

**Airflow equivalent:** Custom UI plugins, external dashboards.

```python
from prefect.artifacts import create_link_artifact, create_table_artifact

@task
def publish_table(inventory: list[dict]) -> None:
    create_table_artifact(key="inventory", table=inventory, description="Inventory levels")

@task
def publish_links() -> None:
    create_link_artifact(key="dashboard", link="https://example.com/dashboard",
                         description="Live dashboard")
```

Table artifacts render as formatted tables. Link artifacts provide quick access
to related resources from the flow run page.

---

### 031 -- Secret Block

**What it demonstrates:** Secure credential management with Prefect's Secret block.

**Airflow equivalent:** Connections / Variables with `is_encrypted`.

```python
from prefect.blocks.system import Secret

@task
def get_api_key() -> str:
    try:
        secret = Secret.load("example-api-key")
        return secret.get()
    except ValueError:
        return "dev-fallback-key-12345"
```

`Secret.load()` retrieves encrypted values from the Prefect server. The fallback
pattern ensures local development works without a configured server.

---

### 032 -- Custom Blocks

**What it demonstrates:** Defining custom Block classes for typed configuration.

**Airflow equivalent:** Custom connection types, configuration classes.

```python
from prefect.blocks.core import Block

class DatabaseConfig(Block):
    host: str = "localhost"
    port: int = 5432
    database: str = "mydb"
    username: str = "admin"

@task
def connect_database(config: DatabaseConfig) -> str:
    return f"Connected to {config.host}:{config.port}/{config.database}"
```

Custom blocks provide typed, validated configuration. In production, save with
`block.save("name")` and load with `Block.load("name")`. Here blocks are
constructed directly for local testability.

---

## Async Patterns (033--036)

### 033 -- Async Tasks

**What it demonstrates:** Async task and flow definitions with sequential awaiting.

**Airflow equivalent:** Deferrable operators (async sensor pattern).

```python
@task
async def async_fetch(url: str) -> dict:
    await asyncio.sleep(0.1)
    return {"url": url, "status": 200}

@flow(name="033_async_tasks", log_prints=True)
async def async_tasks_flow() -> None:
    response = await async_fetch("https://api.example.com/users")
    await async_process(response)
```

Async tasks and flows are defined with `async def` and awaited. The `__main__`
block uses `asyncio.run()`.

---

### 034 -- Concurrent Async

**What it demonstrates:** Concurrent task execution with `asyncio.gather()`.

**Airflow equivalent:** Multiple deferrable operators running in parallel.

```python
@flow(name="034_concurrent_async", log_prints=True)
async def concurrent_async_flow() -> None:
    results = await asyncio.gather(
        fetch_endpoint("users", delay=0.3),
        fetch_endpoint("orders", delay=0.5),
        fetch_endpoint("products", delay=0.2),
    )
    await aggregate_results(list(results))
```

`asyncio.gather()` runs all fetches concurrently. Total wall-clock time is
approximately `max(delays)`, not `sum(delays)`.

---

### 035 -- Async Flow Patterns

**What it demonstrates:** Mixing sync and async tasks in an async flow.

**Airflow equivalent:** Mix of standard and deferrable operators.

```python
@flow(name="035_async_flow_patterns", log_prints=True)
async def async_flow_patterns_flow() -> None:
    raw = sync_extract()              # sync task
    enriched = await enrich_subflow(raw)  # async subflow with gather
    sync_load(enriched)               # sync task
```

Sync tasks are called normally inside async flows. Async subflows use
`asyncio.gather()` for concurrent fan-out over records.

---

### 036 -- Async Map and Submit

**What it demonstrates:** `.map()` and `.submit()` with async tasks.

**Airflow equivalent:** Dynamic task mapping with deferrable operators.

```python
@flow(name="036_async_map_and_submit", log_prints=True)
async def async_map_and_submit_flow() -> None:
    transform_futures = async_transform.map(items)
    transformed = [f.result() for f in transform_futures]

    validate_futures = [async_validate.submit(item) for item in transformed]
    validations = [f.result() for f in validate_futures]
```

`.map()` and `.submit()` work with async tasks for parallel fan-out within
an async flow.

---

## Deployment and Scheduling (037--040)

### 037 -- Flow Serve

**What it demonstrates:** The simplest deployment method: `flow.serve()`.

**Airflow equivalent:** DAG placed in `dags/` folder, picked up by scheduler.

```python
@flow(name="037_flow_serve", log_prints=True)
def flow_serve_flow() -> None:
    raw = extract_data()
    transformed = transform_data(raw)
    load_data(transformed)

# Deploy with: flow_serve_flow.serve(name="037-flow-serve", cron="*/5 * * * *")
```

`flow.serve()` creates a lightweight deployment that runs locally. Pass `cron=`
or `interval=` for scheduling. For production infrastructure isolation, use
`flow.deploy()` with work pools.

---

### 038 -- Schedules

**What it demonstrates:** Schedule types for Prefect deployments.

**Airflow equivalent:** DAG `schedule_interval` (cron, timedelta, timetable).

```python
# CronSchedule: daily_report_flow.serve(name="daily", cron="0 6 * * *")
# IntervalSchedule: interval_check_flow.serve(name="interval", interval=900)
# RRuleSchedule: custom_flow.serve(name="custom", rrule="FREQ=WEEKLY;BYDAY=MO,WE,FR")
```

Three schedule types: `CronSchedule` for cron expressions, `IntervalSchedule`
for fixed intervals, and `RRuleSchedule` for complex recurrence rules. All can
be passed to `flow.serve(schedule=...)` or `flow.deploy(schedule=...)`.

---

### 039 -- Work Pools

**What it demonstrates:** Work pool concepts for production deployments.

**Airflow equivalent:** Executors (Local, Celery, Kubernetes).

```python
# Deploy to a work pool:
# work_pools_flow.deploy(name="039-work-pool", work_pool_name="my-pool")
# Start a worker:
# prefect worker start --pool "my-pool"
```

Work pools define WHERE work runs. Types include `process`, `docker`, and
`kubernetes`. `flow.deploy()` targets a named pool. Workers are long-running
processes that poll a work pool for scheduled runs.

---

### 040 -- Production Pipeline

**What it demonstrates:** Capstone flow combining all Phase 2 concepts.

**Airflow equivalent:** Production DAG with sensors, retries, SLAs, callbacks.

```python
@flow(name="040_production_pipeline", log_prints=True)
def production_pipeline() -> None:
    with tags("production", "phase-2"):
        raw = extract_stage()           # tagged subflow
        transformed = transform_stage(raw)  # retries + caching
        summary = load_stage(transformed)   # persist_result
        notify(summary)                     # markdown artifact
```

This is the capstone flow for Phase 2, combining task caching (`INPUTS` policy),
retries, markdown artifacts, tags, result persistence, and structured logging
into a production-ready pipeline.

---

## Pydantic and Data Patterns (041--044)

### 041 -- Pydantic Models

**What it demonstrates:** Using Pydantic `BaseModel` as task parameters and return types for automatic validation and type safety.

**Airflow equivalent:** XCom push/pull with complex types (JSON/pickle serialisation).

```python
from pydantic import BaseModel

class UserRecord(BaseModel):
    name: str
    email: str
    age: int

@task
def extract_users(config: PipelineConfig) -> list[UserRecord]:
    raw = [{"name": "Alice", "email": "alice@example.com", "age": 30}]
    return [UserRecord(**r) for r in raw[:config.batch_size]]
```

Pydantic models flow naturally between tasks -- no XCom serialisation pain. Validation happens automatically on construction.

---

### 042 -- Shell Tasks

**What it demonstrates:** Running shell commands and scripts from Prefect tasks using `subprocess`.

**Airflow equivalent:** BashOperator.

```python
@task
def run_command(cmd: str) -> str:
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
    return result.stdout.strip()
```

Prefect has no BashOperator. `subprocess.run()` inside a `@task` is the direct equivalent.

---

### 043 -- HTTP Tasks

**What it demonstrates:** Making HTTP requests from tasks using `httpx`.

**Airflow equivalent:** HttpOperator, HttpSensor.

```python
@task
def http_get(url: str) -> dict:
    response = httpx.get(url, timeout=10.0)
    response.raise_for_status()
    return response.json()
```

No special operator needed. `httpx` (a Prefect transitive dependency) in a `@task` replaces HttpOperator entirely.

---

### 044 -- Task Factories

**What it demonstrates:** Creating reusable tasks dynamically with factory functions.

**Airflow equivalent:** Custom operators, `@task.bash` decorator variants.

```python
def make_extractor(source: str):
    @task(name=f"extract_{source}")
    def extract() -> dict:
        return {"source": source, "records": [...]}
    return extract

extract_api = make_extractor("api")
extract_database = make_extractor("database")
```

Factory functions generate `@task`-decorated callables for consistent behaviour across different data sources.

---

## Advanced Mapping and Error Handling (045--048)

### 045 -- Advanced Map Patterns

**What it demonstrates:** Multi-argument `.map()`, chained maps, and result collection.

**Airflow equivalent:** `expand_kwargs()`, `partial().expand()`.

```python
station_futures = process_station.map(
    [s["station_id"] for s in stations],
    [s["lat"] for s in stations],
    [s["lon"] for s in stations],
)
station_results = [f.result() for f in station_futures]
```

Unpack list-of-dicts into parallel `.map()` calls by passing separate lists for each parameter.

---

### 046 -- Error Handling ETL

**What it demonstrates:** The quarantine pattern -- good rows pass through, bad rows are captured with error reasons.

**Airflow equivalent:** Error handling with quarantine pattern.

```python
class QuarantineResult(BaseModel):
    good_records: list[dict]
    bad_records: list[dict]
    errors: list[str]

@task
def process_with_quarantine(records: list[dict]) -> QuarantineResult:
    good, bad, errors = [], [], []
    for record in records:
        try:
            validate(record)
            good.append(record)
        except ValueError as e:
            bad.append(record)
            errors.append(str(e))
    return QuarantineResult(good_records=good, bad_records=bad, errors=errors)
```

Pydantic models make quarantine results structured and type-safe.

---

### 047 -- Pydantic Validation

**What it demonstrates:** Using Pydantic `field_validator` for data quality enforcement.

**Airflow equivalent:** Schema validation pipeline.

```python
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

`field_validator` replaces manual schema checking code. Invalid data raises a `ValidationError` automatically.

---

### 048 -- SLA Monitoring

**What it demonstrates:** Tracking task durations and comparing against SLA thresholds.

**Airflow equivalent:** SLA miss detection, `execution_timeout`.

```python
@task
def sla_report(results: list[dict], thresholds: dict | None = None) -> str:
    for result in results:
        duration = result["duration"]
        limit = thresholds.get(result["task"], 1.0)
        status = "OK" if duration <= limit else "BREACH"
```

Use `time.monotonic()` for accurate timing and compare against configurable thresholds.

---

## Notifications and Observability (049--052)

### 049 -- Webhook Notifications

**What it demonstrates:** Sending webhook notifications on pipeline events.

**Airflow equivalent:** Webhook alerts on pipeline events.

```python
@flow(
    name="049_webhook_notifications",
    on_completion=[on_flow_completion],
    on_failure=[on_flow_failure],
)
def webhook_notifications_flow() -> None:
    send_notification("pipeline.started", {"source": "demo"})
    result = process_data()
    send_notification("pipeline.completed", result)
```

Flow hooks (`on_completion`, `on_failure`) trigger automatically. In production, `send_notification` would POST to Slack, PagerDuty, etc.

---

### 050 -- Failure Escalation

**What it demonstrates:** Progressive retry with escalation hooks at each failure.

**Airflow equivalent:** Progressive retry with escalating callbacks.

```python
@task(retries=3, retry_delay_seconds=0, on_failure=[on_task_failure])
def flaky_task(fail_count: int = 2) -> str:
    ...
```

The `on_failure` hook fires on each retry failure, allowing escalation logging. After all retries exhaust, the flow-level `on_completion` hook reports the final outcome.

---

### 051 -- Testable Flow Patterns

**What it demonstrates:** Separating business logic from Prefect wiring for maximum testability.

**Airflow equivalent:** Thin DAG wiring with logic in external modules.

```python
# Pure function (no Prefect imports)
def _validate_record(record: dict) -> dict:
    if not record.get("name"):
        raise ValueError("missing name")
    return {**record, "valid": True}

# Thin task wrapper
@task
def validate(record: dict) -> dict:
    return _validate_record(record)
```

Test pure functions directly (fast, no Prefect overhead) and task wrappers via `.fn()`.

---

### 052 -- Reusable Utilities

**What it demonstrates:** Custom task utility decorators for consistent behaviour.

**Airflow equivalent:** Custom hooks and sensors.

```python
def timed_task(fn):
    @task(name=fn.__name__)
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        result = fn(*args, **kwargs)
        result["_duration"] = round(time.monotonic() - start, 4)
        return result
    return wrapper

@timed_task
def compute_metric(name: str, value: float) -> dict:
    return {"name": name, "value": value * 1.1, "unit": "ops/sec"}
```

Build a task utility library for timing, validation, and other cross-cutting concerns.

---

## Composition and Scheduling (053--056)

### 053 -- Advanced State Handling

**What it demonstrates:** Using `allow_failure` and state inspection for mixed-outcome workflows.

**Airflow equivalent:** Trigger rules (`all_success`, `all_done`, etc.).

```python
from prefect import allow_failure

fail_future = fail_task.submit()
skip_task(wait_for=[allow_failure(fail_future)])
```

`allow_failure` lets downstream tasks run even when upstream tasks fail. Combine with state inspection for conditional logic.

---

### 054 -- Nested Subflows

**What it demonstrates:** Organising complex pipelines with hierarchical subflow groups.

**Airflow equivalent:** TaskGroups and nested groups.

```python
@flow(name="054_nested_subflows", log_prints=True)
def nested_subflows_flow() -> None:
    raw = extract_group()       # subflow with multiple tasks
    transformed = transform_group(raw)  # subflow with clean + enrich
    load_group(transformed)     # subflow with write + verify
```

Each subflow appears as a nested flow run in the Prefect UI with independent state tracking -- the equivalent of Airflow TaskGroups.

---

### 055 -- Backfill Patterns

**What it demonstrates:** Parameterised pipelines for date-range processing with gap detection.

**Airflow equivalent:** Backfill awareness, parameterised pipelines.

```python
@flow(name="055_backfill_patterns", log_prints=True)
def backfill_patterns_flow(start_date: str = "2024-01-01", end_date: str = "2024-01-05"):
    initial_results = process_date.map(initial_dates)
    gaps = detect_gaps(initial_dates, start_date, end_date)
    backfill_results = process_date.map(gaps)
```

Flow parameters replace Airflow's `logical_date`. Gap detection identifies missing dates for incremental backfill.

---

### 056 -- Runtime Context

**What it demonstrates:** Accessing flow and task run metadata at runtime.

**Airflow equivalent:** Jinja templating (`{{ ds }}`), macros, runtime info.

```python
from prefect.runtime import flow_run, task_run

@task
def get_flow_info() -> dict:
    return {
        "flow_run_name": flow_run.name,
        "flow_name": flow_run.flow_name,
    }
```

`prefect.runtime` provides access to flow run ID, name, parameters, and tags -- replacing Airflow's Jinja template variables.

---

## Advanced Features and Capstone (057--060)

### 057 -- Transactions

**What it demonstrates:** Atomic task groups with rollback on failure using Prefect transactions.

**Airflow equivalent:** No direct equivalent -- Prefect-specific feature.

```python
from prefect.transactions import transaction

@flow(name="057_transactions", log_prints=True)
def transactions_flow() -> None:
    with transaction():
        a = step_a()
        b = step_b()
        c = step_c()
    summarize_transaction([a, b, c])
```

The `transaction()` context manager groups tasks atomically. This is a unique Prefect advantage with no Airflow equivalent.

---

### 058 -- Interactive Flows

**What it demonstrates:** Human-in-the-loop approval patterns.

**Airflow equivalent:** Human-in-the-loop operators.

```python
@flow(name="058_interactive_flows", log_prints=True)
def interactive_flows_flow() -> None:
    data = prepare_data()
    approved = mock_approval(data)  # In production: pause_flow_run()
    if approved:
        publish(data)
    else:
        archive(data)
```

In production, use `pause_flow_run()` to pause and wait for human input via the Prefect UI. The mock approval pattern enables local testing.

---

### 059 -- Task Runners

**What it demonstrates:** Comparing thread pool and default task runners for different workloads.

**Airflow equivalent:** Executors (Local, Celery, Kubernetes).

```python
from prefect.task_runners import ThreadPoolTaskRunner

@flow(task_runner=ThreadPoolTaskRunner(max_workers=3))
def threaded_io_flow() -> str:
    futures = io_bound_task.map(items)
    return summarize_runner([f.result() for f in futures], "ThreadPool")
```

`ThreadPoolTaskRunner` provides concurrent execution for I/O-bound tasks. The default runner handles CPU-bound work.

---

### 060 -- Production Pipeline v2

**What it demonstrates:** Capstone flow combining all Phase 3 features into a production-ready pipeline.

**Airflow equivalent:** Full ETL SCD capstone.

```python
@flow(name="060_production_pipeline_v2", log_prints=True, on_completion=[on_pipeline_completion])
def production_pipeline_v2_flow() -> None:
    with tags("production", "phase-3"):
        source_records = extract_stage()
        with transaction():
            validated = validate_stage(source_records)
        transformed = transform_stage(validated)
        metrics = compute_metrics(transformed)
        publish_summary(metrics)
```

This capstone combines Pydantic models with field validators, transactions, retries, markdown artifacts, tags, state hooks, and `.map()` into a realistic production pipeline.
