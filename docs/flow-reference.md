# Flow Reference

Detailed walkthrough of all 40 example flows, organised by category.

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
