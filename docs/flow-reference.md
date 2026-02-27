# Flow Reference

Detailed walkthrough of all 113 example flows, organised by category.

---

## Basics 

### Hello World

**What it demonstrates:** The simplest possible Prefect flow -- two tasks
executed sequentially.

**Airflow equivalent:** BashOperator tasks with `>>` dependency.

```python
@task
def say_hello() -> str:
    msg = "Hello from Prefect!"
    print(msg)
    return msg

@flow(name="basics_hello_world", log_prints=True)
def hello_world() -> None:
    say_hello()
    print_date()
```

Tasks are plain Python functions. Call them inside a `@flow` and Prefect tracks
everything automatically.

---

### Python Tasks

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

### Task Dependencies

**What it demonstrates:** Parallel fan-out with `.submit()` and a join step.

**Airflow equivalent:** `>>` operator / `set_downstream`.

```python
@flow(name="basics_task_dependencies", log_prints=True)
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

### Taskflow ETL

**What it demonstrates:** Classic extract-transform-load wired through return
values.

**Airflow equivalent:** TaskFlow API (`@task`).

```python
@flow(name="basics_taskflow_etl", log_prints=True)
def taskflow_etl_flow() -> None:
    raw = extract()
    transformed = transform(raw)
    load(transformed)
```

Prefect is natively taskflow-first. Each task returns data and the next task
receives it -- no XCom needed.

---

### Task Results

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

## Control Flow 

### Conditional Logic

**What it demonstrates:** Branching with plain Python `if/elif/else`.

**Airflow equivalent:** BranchPythonOperator.

```python
@flow(name="basics_conditional_logic", log_prints=True)
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

### State Handlers

**What it demonstrates:** Reacting to task/flow state changes with hook
functions, and continuing past failures with `allow_failure`.

**Airflow equivalent:** `on_failure_callback` / trigger_rule.

```python
@task(on_failure=[on_task_failure])
def fail_task():
    raise ValueError("Intentional failure for demonstration")

@flow(name="basics_state_handlers", log_prints=True, on_completion=[on_flow_completion])
def state_handlers_flow() -> None:
    succeed_task()
    failing_future = fail_task.submit()
    always_run_task(wait_for=[allow_failure(failing_future)])
```

Hooks are plain functions (not tasks) that receive `task`, `task_run`, and
`state`. `allow_failure` lets downstream tasks run even when upstream tasks
fail.

---

### Parameterized Flows

**What it demonstrates:** Runtime parameters with typed defaults.

**Airflow equivalent:** Jinja2 templating / params dict.

```python
@flow(name="basics_parameterized_flows", log_prints=True)
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

## Composition 

### Subflows

**What it demonstrates:** Composing larger pipelines from smaller, reusable
flows.

**Airflow equivalent:** TaskGroup / SubDagOperator.

```python
@flow(name="basics_subflows", log_prints=True)
def pipeline_flow() -> None:
    raw = extract_flow()
    transformed = transform_flow(raw)
    load_flow(transformed)
```

A `@flow` can call other `@flow` functions. Each subflow appears as a nested
flow run in the Prefect UI with its own state tracking.

---

### Dynamic Tasks

**What it demonstrates:** Dynamic fan-out over a list of items with `.map()`.

**Airflow equivalent:** Dynamic task mapping (`expand()`).

```python
@flow(name="basics_dynamic_tasks", log_prints=True)
def dynamic_tasks_flow() -> None:
    items = generate_items()
    processed = process_item.map(items)
    summarize(processed)
```

`.map()` creates one task run per item. The number of items can vary at
runtime -- no DAG rewrite required.

---

## Operational 

### Polling Tasks

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

### Retries and Hooks

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

## Reuse and Events 

### Reusable Tasks

**What it demonstrates:** Importing shared tasks from a project task library.

**Airflow equivalent:** Custom operators / shared utils.

```python
from prefect_examples.tasks import print_message, square_number

@flow(name="basics_reusable_tasks", log_prints=True)
def reusable_tasks_flow() -> None:
    print_message("Hello from reusable tasks!")
    result = square_number(7)
```

Tasks are just Python functions. Import them from a shared module and call them
in any flow. The shared library lives in `src/prefect_examples/tasks.py`.

---

### Events

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

## Advanced 

### Flow of Flows

**What it demonstrates:** Orchestrating multiple flows from a parent flow.

**Airflow equivalent:** TriggerDagRunOperator.

```python
@flow(name="basics_flow_of_flows", log_prints=True)
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

### Concurrency Limits

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

### Variables and Params

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

### Early Return

**What it demonstrates:** Short-circuiting a flow with a plain `return`.

**Airflow equivalent:** ShortCircuitOperator.

```python
@flow(name="basics_early_return", log_prints=True)
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

### Context Managers

**What it demonstrates:** Resource setup and teardown with `try/finally`.

**Airflow equivalent:** `@setup` / `@teardown` decorators.

```python
@flow(name="basics_context_managers", log_prints=True)
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

### Complex Pipeline

**What it demonstrates:** End-to-end pipeline combining subflows, mapped tasks,
and notifications.

**Airflow equivalent:** Complex DAG with branching, sensors, callbacks.

```python
@flow(name="basics_complex_pipeline", log_prints=True)
def complex_pipeline() -> None:
    raw = extract_stage()
    transformed = transform_stage(raw)
    summary = load_stage(transformed)
    notify(summary)
```

The transform stage uses chained `.map()` calls:

```python
@flow(name="basics_transform", log_prints=True)
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

### Task Caching

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

### Task Timeouts

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

@flow(name="core_task_timeouts", log_prints=True, timeout_seconds=30)
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

### Task Run Names

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

### Advanced Retries

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

### Structured Logging

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

### Tags

**What it demonstrates:** Tagging tasks and flows for organisation and filtering.

**Airflow equivalent:** DAG/task tags for filtering in the UI.

```python
from prefect import flow, tags, task

@task(tags=["etl", "extract"])
def extract_sales() -> list[dict]: ...

@flow(name="core_tags", log_prints=True, tags=["examples", "phase-2"])
def tags_flow() -> None:
    extract_sales()
    with tags("ad-hoc", "debug"):
        generic_task("debug-data")
```

Static tags are set on decorators. The `tags()` context manager adds runtime
tags to all tasks within its scope. Tags are visible in the Prefect UI and can
be used for filtering and automation rules.

---

### Flow Run Names

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

### Result Persistence

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

## Artifacts and Blocks 

### Markdown Artifacts

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

### Table and Link Artifacts

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

### Secret Block

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

### Custom Blocks

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

## Async Patterns 

### Async Tasks

**What it demonstrates:** Async task and flow definitions with sequential awaiting.

**Airflow equivalent:** Deferrable operators (async sensor pattern).

```python
@task
async def async_fetch(url: str) -> dict:
    await asyncio.sleep(0.1)
    return {"url": url, "status": 200}

@flow(name="core_async_tasks", log_prints=True)
async def async_tasks_flow() -> None:
    response = await async_fetch("https://api.example.com/users")
    await async_process(response)
```

Async tasks and flows are defined with `async def` and awaited. The `__main__`
block uses `asyncio.run()`.

---

### Concurrent Async

**What it demonstrates:** Concurrent task execution with `asyncio.gather()`.

**Airflow equivalent:** Multiple deferrable operators running in parallel.

```python
@flow(name="core_concurrent_async", log_prints=True)
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

### Async Flow Patterns

**What it demonstrates:** Mixing sync and async tasks in an async flow.

**Airflow equivalent:** Mix of standard and deferrable operators.

```python
@flow(name="core_async_flow_patterns", log_prints=True)
async def async_flow_patterns_flow() -> None:
    raw = sync_extract()              # sync task
    enriched = await enrich_subflow(raw)  # async subflow with gather
    sync_load(enriched)               # sync task
```

Sync tasks are called normally inside async flows. Async subflows use
`asyncio.gather()` for concurrent fan-out over records.

---

### Async Map and Submit

**What it demonstrates:** `.map()` and `.submit()` with async tasks.

**Airflow equivalent:** Dynamic task mapping with deferrable operators.

```python
@flow(name="core_async_map_and_submit", log_prints=True)
async def async_map_and_submit_flow() -> None:
    transform_futures = async_transform.map(items)
    transformed = [f.result() for f in transform_futures]

    validate_futures = [async_validate.submit(item) for item in transformed]
    validations = [f.result() for f in validate_futures]
```

`.map()` and `.submit()` work with async tasks for parallel fan-out within
an async flow.

---

## Deployment and Scheduling 

### Flow Serve

**What it demonstrates:** The simplest deployment method: `flow.serve()`.

**Airflow equivalent:** DAG placed in `dags/` folder, picked up by scheduler.

```python
@flow(name="core_flow_serve", log_prints=True)
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

### Schedules

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

### Work Pools

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

### Production Pipeline

**What it demonstrates:** Capstone flow combining all Phase 2 concepts.

**Airflow equivalent:** Production DAG with sensors, retries, SLAs, callbacks.

```python
@flow(name="core_production_pipeline", log_prints=True)
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

## Pydantic and Data Patterns 

### Pydantic Models

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

### Shell Tasks

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

### HTTP Tasks

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

### Task Factories

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

## Advanced Mapping and Error Handling 

### Advanced Map Patterns

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

### Error Handling ETL

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

### Pydantic Validation

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

### SLA Monitoring

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

## Notifications and Observability 

### Webhook Notifications

**What it demonstrates:** Sending webhook notifications on pipeline events.

**Airflow equivalent:** Webhook alerts on pipeline events.

```python
@flow(
    name="patterns_webhook_notifications",
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

### Notification Blocks

**What it demonstrates:** Using Prefect's built-in `SlackWebhook` and
`CustomWebhookNotificationBlock` for pipeline alerting with a unified
`notify(body, subject)` interface.

**Airflow equivalent:** Slack/email/PagerDuty callbacks via operators.

```python
@task
def configure_notification_blocks() -> dict[str, Any]:
    slack = SlackWebhook(url=SecretStr("https://hooks.slack.com/services/T00/B00/xxxx"))

    custom = CustomWebhookNotificationBlock(
        name="ops-webhook",
        url="https://monitoring.example.com/alerts",
        method="POST",
        json_data={"text": "{{subject}}: {{body}}"},
        secrets={"api_token": "placeholder-token"},
    )
    return {"slack": type(slack).__name__, "custom": type(custom).__name__}

@task
def demonstrate_template_resolution() -> dict[str, Any]:
    block = CustomWebhookNotificationBlock(
        name="template-demo",
        url="https://api.example.com/notify?token={{api_token}}",
        method="POST",
        json_data={
            "title": "{{subject}}",
            "message": "{{body}}",
            "source": "{{name}}",
            "auth": "Bearer {{api_token}}",
        },
        secrets={"api_token": "secret-xyz-789"},
    )
    return block._build_request_args(
        body="Pipeline completed: 150 records processed",
        subject="Pipeline Alert",
    )

def on_completion_notify(flow, flow_run, state):
    # SlackWebhook.load("prod-slack").notify(
    #     body=f"Flow {flow_run.name!r} completed.", subject="Flow Completed")
    print(f"HOOK  Flow {flow_run.name!r} completed -- would notify via SlackWebhook")

@flow(
    name="patterns_notification_blocks",
    log_prints=True,
    on_completion=[on_completion_notify],
    on_failure=[on_failure_notify],
)
def notification_blocks_flow() -> None:
    channels = configure_notification_blocks()
    for source in ["api", "database", "file"]:
        process_data(source)
    demonstrate_template_resolution()
```

All notification blocks share the same `notify(body, subject)` method.
`SlackWebhook` sends to a Slack webhook URL; `CustomWebhookNotificationBlock`
sends to any HTTP endpoint with template resolution for `{{subject}}`,
`{{body}}`, `{{name}}`, and custom `secrets` keys. Flow hooks wire these
blocks to lifecycle events for automatic alerting in production.

---

### Webhook Block

**What it demonstrates:** Using the built-in `Webhook` block for configurable
outbound HTTP calls with stored credentials.

**Airflow equivalent:** `SimpleHttpOperator` with connection credentials.

```python
@task
def create_post_webhook() -> dict[str, Any]:
    webhook = Webhook(
        method="POST",
        url=SecretStr("https://api.example.com/events"),
        headers=SecretDict({
            "Content-Type": "application/json",
            "Authorization": "Bearer placeholder-token",
        }),
    )
    summary = {
        "method": webhook.method,
        "url_host": "api.example.com",
        "header_keys": list(webhook.headers.get_secret_value().keys()),
    }
    print(f"POST webhook configured: {summary}")
    return summary

@task
def simulate_webhook_call(
    method: str, url_host: str, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    result = {
        "method": method,
        "url_host": url_host,
        "payload": payload,
        "simulated": True,
    }
    print(f"Simulated {method} to {url_host} -- payload: {payload}")
    return result

@flow(name="core_webhook_block", log_prints=True)
def webhook_block_flow() -> dict[str, Any]:
    post_summary = create_post_webhook()
    post_result = simulate_webhook_call(
        method=post_summary["method"],
        url_host=post_summary["url_host"],
        payload={"event": "pipeline.completed", "records": 150},
    )
    pattern = demonstrate_save_load_pattern()
    return {"post_result": post_result, "persistence_pattern": pattern}
```

The `Webhook` block stores URL, method, headers, and auth in a reusable,
server-persisted block. It powers the `CallWebhook` automation action and can
replace ad-hoc `httpx.post()` calls with a managed, auditable configuration.

---

### Failure Escalation

**What it demonstrates:** Progressive retry with escalation hooks at each failure.

**Airflow equivalent:** Progressive retry with escalating callbacks.

```python
@task(retries=3, retry_delay_seconds=0, on_failure=[on_task_failure])
def flaky_task(fail_count: int = 2) -> str:
    ...
```

The `on_failure` hook fires on each retry failure, allowing escalation logging. After all retries exhaust, the flow-level `on_completion` hook reports the final outcome.

---

### Testable Flow Patterns

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

### Reusable Utilities

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

## Composition and Scheduling 

### Advanced State Handling

**What it demonstrates:** Using `allow_failure` and state inspection for mixed-outcome workflows.

**Airflow equivalent:** Trigger rules (`all_success`, `all_done`, etc.).

```python
from prefect import allow_failure

fail_future = fail_task.submit()
skip_task(wait_for=[allow_failure(fail_future)])
```

`allow_failure` lets downstream tasks run even when upstream tasks fail. Combine with state inspection for conditional logic.

---

### Nested Subflows

**What it demonstrates:** Organising complex pipelines with hierarchical subflow groups.

**Airflow equivalent:** TaskGroups and nested groups.

```python
@flow(name="patterns_nested_subflows", log_prints=True)
def nested_subflows_flow() -> None:
    raw = extract_group()       # subflow with multiple tasks
    transformed = transform_group(raw)  # subflow with clean + enrich
    load_group(transformed)     # subflow with write + verify
```

Each subflow appears as a nested flow run in the Prefect UI with independent state tracking -- the equivalent of Airflow TaskGroups.

---

### Backfill Patterns

**What it demonstrates:** Parameterised pipelines for date-range processing with gap detection.

**Airflow equivalent:** Backfill awareness, parameterised pipelines.

```python
@flow(name="patterns_backfill_patterns", log_prints=True)
def backfill_patterns_flow(start_date: str = "2024-01-01", end_date: str = "2024-01-05"):
    initial_results = process_date.map(initial_dates)
    gaps = detect_gaps(initial_dates, start_date, end_date)
    backfill_results = process_date.map(gaps)
```

Flow parameters replace Airflow's `logical_date`. Gap detection identifies missing dates for incremental backfill.

---

### Runtime Context

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

## Advanced Features and Capstone 

### Transactions

**What it demonstrates:** Atomic task groups with rollback on failure using Prefect transactions.

**Airflow equivalent:** No direct equivalent -- Prefect-specific feature.

```python
from prefect.transactions import transaction

@flow(name="patterns_transactions", log_prints=True)
def transactions_flow() -> None:
    with transaction():
        a = step_a()
        b = step_b()
        c = step_c()
    summarize_transaction([a, b, c])
```

The `transaction()` context manager groups tasks atomically. This is a unique Prefect advantage with no Airflow equivalent.

---

### Interactive Flows

**What it demonstrates:** Human-in-the-loop approval patterns.

**Airflow equivalent:** Human-in-the-loop operators.

```python
@flow(name="patterns_interactive_flows", log_prints=True)
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

### Task Runners

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

### Production Pipeline v2

**What it demonstrates:** Capstone flow combining all Phase 3 features into a production-ready pipeline.

**Airflow equivalent:** Full ETL SCD capstone.

```python
@flow(name="patterns_production_pipeline_v2", log_prints=True, on_completion=[on_pipeline_completion])
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

---

## File I/O Patterns 

### CSV File Processing

**What it demonstrates:** File-based ETL pipeline using the stdlib `csv` module
with generate, read, validate, transform, write, and archive steps.

**Airflow equivalent:** CSV landing zone pipeline (DAG 063).

```python
@task
def validate_csv_row(row: dict, row_number: int, required_columns: list[str]) -> CsvRecord:
    errors = []
    for col in required_columns:
        if col not in row or not row[col].strip():
            errors.append(f"Missing or empty required column: {col}")
    return CsvRecord(row_number=row_number, data=row, valid=len(errors) == 0, errors=errors)
```

`csv.DictReader` and `csv.DictWriter` replace external CSV libraries.
`tempfile.mkdtemp()` provides isolated working directories.

---

### JSON Event Ingestion

**What it demonstrates:** Recursive nested JSON flattening into dot-separated
keys with NDJSON (newline-delimited JSON) output.

**Airflow equivalent:** JSON event stream to Parquet (DAG 064).

```python
@task
def flatten_dict(data: dict, prefix: str = "", separator: str = ".") -> dict:
    items = {}
    for key, value in data.items():
        new_key = f"{prefix}{separator}{key}" if prefix else key
        if isinstance(value, dict):
            items.update(flatten_dict.fn(value, new_key, separator))
        else:
            items[new_key] = value
    return items
```

Recursive flattening handles arbitrarily nested structures. NDJSON output
writes one JSON object per line for streaming consumption.

---

### Multi-File Batch Processing

**What it demonstrates:** Mixed CSV+JSON batch processing with file-type
dispatch, column harmonisation, and hash-based deduplication.

**Airflow equivalent:** Mixed CSV+JSON batch processing (DAG 065).

```python
@task
def read_file(path: Path) -> list[dict]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with open(path, newline="") as f:
            return list(csv.DictReader(f))
    elif suffix == ".json":
        return json.loads(path.read_text())
```

File suffix determines the reader. Column harmonisation maps different schemas
to a unified format. Hash dedup uses `hashlib.sha256` on key fields.

---

### Incremental Processing

**What it demonstrates:** Manifest-based incremental file processing. A JSON
manifest tracks which files have been processed; re-runs skip them.

**Airflow equivalent:** Manifest-based incremental file processing (DAG 067).

```python
@task
def identify_new_files(all_files: list[Path], manifest: ProcessingManifest) -> list[Path]:
    return [f for f in all_files if f.name not in manifest.processed_files]
```

Run the flow twice: the second run processes zero files because the manifest
already records them. This is the foundation for idempotent file pipelines.

---

## Data Quality Framework 

### Quality Rules Engine

**What it demonstrates:** Configuration-driven data quality rules with a
registry pattern and traffic-light scoring (green/amber/red).

**Airflow equivalent:** Freshness and completeness checks (DAG 070).

```python
@task
def execute_rule(data: list[dict], rule: QualityRule) -> RuleResult:
    if rule.rule_type == "not_null":
        return run_not_null_check.fn(data, rule.column)
    elif rule.rule_type == "range":
        return run_range_check.fn(data, rule.column, ...)
```

Rules are defined as config dicts, parsed into Pydantic models, and dispatched
to check functions. The overall score determines the traffic light.

---

### Cross-Dataset Validation

**What it demonstrates:** Referential integrity checks between related datasets
(orders, customers, products) with orphan detection.

**Airflow equivalent:** Referential integrity checks (DAG 071).

```python
@task
def check_referential_integrity(child_data, parent_data, child_key, parent_key, check_name):
    parent_values = {row[parent_key] for row in parent_data}
    orphan_keys = [row[child_key] for row in child_data if row[child_key] not in parent_values]
    return IntegrityResult(orphan_count=len(orphan_keys), passed=len(orphan_keys) == 0, ...)
```

Foreign key validation is a pure Python set operation. The test data deliberately
includes orphan records to demonstrate detection.

---

### Data Profiling

**What it demonstrates:** Statistical data profiling using the stdlib
`statistics` module (mean, stdev, median) with column-level type inference.

**Airflow equivalent:** Consolidated quality dashboard (DAG 072).

```python
@task
def profile_numeric_column(name: str, values: list) -> ColumnProfile:
    non_null = [float(v) for v in values if v is not None]
    return ColumnProfile(
        name=name, dtype="numeric",
        mean=round(statistics.mean(non_null), 4),
        stdev=round(statistics.stdev(non_null), 4),
        median=round(statistics.median(non_null), 4), ...
    )
```

Column type is inferred from values. Numeric columns get statistical profiles;
string columns get length and uniqueness counts.

---

### Pipeline Health Monitor

**What it demonstrates:** Meta-monitoring / watchdog pattern. A flow checks
the health of other pipelines' outputs via file existence, freshness, row
counts, and value range checks.

**Airflow equivalent:** Pipeline health check (DAG 076).

```python
@task
def aggregate_health(pipeline_name: str, results: list[HealthCheckResult]) -> PipelineHealthReport:
    if any(r.status == "critical" for r in results):
        overall = "critical"
    elif any(r.status == "degraded" for r in results):
        overall = "degraded"
    else:
        overall = "healthy"
```

Worst-status-wins aggregation ensures a single failing check flags the entire
pipeline.

---

## API Orchestration Patterns 

### Multi-Source Forecast

**What it demonstrates:** Chained `.map()` calls: geocode cities, then fetch
forecasts using the resulting coordinates.

**Airflow equivalent:** Multi-city forecast, geocoding (DAGs 081, 086).

```python
coord_futures = geocode_city.map(cities)
coords = [f.result() for f in coord_futures]
forecast_futures = fetch_forecast.map(coords)
forecasts = [f.result() for f in forecast_futures]
```

The output of one `.map()` feeds into the next. All API calls are deterministic
simulations for offline testing.

---

### API Pagination

**What it demonstrates:** Paginated API consumption with chunked parallel
processing using `.map()`.

**Airflow equivalent:** Chunked API fetching (DAG 094).

```python
@task
def simulate_api_page(page: int, page_size: int, total_records: int) -> PageResponse:
    total_pages = (total_records + page_size - 1) // page_size
    start = (page - 1) * page_size
    end = min(start + page_size, total_records)
    records = [{"id": i + 1, "value": ...} for i in range(start, end)]
    return PageResponse(page=page, records=records, has_next=page < total_pages, ...)
```

Pages are fetched sequentially (next page depends on `has_next`), then records
are chunked and processed in parallel via `.map()`.

---

### Cross-Source Enrichment

**What it demonstrates:** Joining data from three simulated API sources with
graceful degradation on partial enrichment failure.

**Airflow equivalent:** Cross-API enrichment (DAGs 090, 092).

```python
@task
def merge_enrichments(base, demo, fin, geo) -> EnrichedRecord:
    sources_available = sum(1 for s in [demo, fin, geo] if s is not None)
    completeness = sources_available / 3.0
    return EnrichedRecord(..., enrichment_completeness=round(completeness, 2))
```

When an enrichment source returns `None`, the record continues with partial
data. Completeness is tracked per-record and summarised in the report.

---

### Response Caching

**What it demonstrates:** Application-level response cache with TTL expiry,
hashlib-based keys, and hit/miss tracking.

**Airflow equivalent:** Forecast accuracy / cached vs fresh comparison (DAG 082).

```python
@task
def fetch_with_cache(endpoint, params, cache, ttl_seconds=300):
    key = make_cache_key.fn(endpoint, params)
    entry = check_cache.fn(cache, key, ttl_seconds)
    if entry is not None:
        return entry.value, True  # cache hit
    data = simulate_api_call.fn(endpoint, params)
    cache[key] = {"value": data, "cached_at": time.time()}
    return data, False  # cache miss
```

Duplicate requests hit the cache. TTL-based expiry prevents stale data.

---

## Configuration and Orchestration Patterns 

### Config-Driven Pipeline

**What it demonstrates:** Pipeline behaviour controlled entirely by a typed
`PipelineConfig` parameter: stage selection, parameter overrides, conditional
execution. The flow accepts a Pydantic model directly so that Prefect can
auto-generate a rich parameter schema for the UI.

**Airflow equivalent:** API-triggered scheduling with config payload (DAG 109).

```python
@flow(name="data_engineering_config_driven_pipeline", log_prints=True)
def config_driven_pipeline_flow(config: PipelineConfig | None = None) -> PipelineResult:
    ...
```

Different `PipelineConfig` values produce different pipeline runs through the
same flow. Disabled stages are skipped automatically.

---

### Producer-Consumer

**What it demonstrates:** Cross-flow communication via file-based data contracts.
Separate producer and consumer flows connected through data packages.

**Airflow equivalent:** Asset + XCom producer/consumer (DAG 112).

```python
@flow(name="data_engineering_producer_consumer", log_prints=True)
def producer_consumer_flow(work_dir=None):
    producer_flow(data_dir, producer_id="alpha", records=8)
    producer_flow(data_dir, producer_id="beta", records=12)
    results = consumer_flow(data_dir, consumer_id="main_consumer")
```

Producers write JSON data + metadata files. Consumers discover and process
them. Each is independently testable.

---

### Circuit Breaker

**What it demonstrates:** Circuit breaker state machine (closed -> open ->
half_open -> closed). After N consecutive failures, the circuit opens.

**Airflow equivalent:** None (Prefect-native resilience pattern).

```python
@task
def call_with_circuit(circuit: CircuitState, should_succeed: bool):
    if circuit.state == "open":
        circuit = circuit.model_copy(update={"state": "half_open"})
    # ... execute call, track failures, trip if threshold reached
```

Outcomes are a deterministic list of booleans, making the simulation fully
testable and reproducible.

---

### Discriminated Unions

**What it demonstrates:** Pydantic discriminated unions for type-safe
polymorphic event dispatch. The flow accepts a typed `list[Event]` directly,
giving Prefect a rich parameter schema instead of freeform JSON.

**Airflow equivalent:** Multi-API dashboard with heterogeneous sources (DAG 098).

```python
Event = Annotated[Union[EmailEvent, WebhookEvent, ScheduleEvent], Field(discriminator="event_type")]

@flow(name="data_engineering_discriminated_unions", log_prints=True)
def discriminated_unions_flow(events: list[Event] | None = None) -> ProcessingSummary:
    ...
```

The `event_type` literal field acts as a discriminator. Passing typed model
instances directly avoids manual dict parsing inside the flow.

---

## Production Patterns and Capstone 

### Streaming Batch Processor

**What it demonstrates:** Windowed batch processing with anomaly detection
(values > 3 stdev from window mean) and trend analysis between windows.

**Airflow equivalent:** GeoJSON parsing, OData pivoting (DAGs 091, 095).

```python
@task
def process_window(data: list[dict], window: BatchWindow) -> WindowResult:
    values = [r["value"] for r in data[window.start_index:window.end_index]]
    mean = statistics.mean(values)
    stdev = statistics.stdev(values)
    anomalies = [r for r in records if abs(r["value"] - mean) > 3 * stdev]
```

Windows are processed in parallel via `.map()`. Seeded random ensures
reproducible test data.

---

### Idempotent Operations

**What it demonstrates:** Hash-based idempotency registry. Operations check
the registry before executing, making them safe to re-run.

**Airflow equivalent:** None (production resilience pattern).

```python
@task
def idempotent_execute(registry, name, inputs):
    op_id = compute_operation_id.fn(name, inputs)
    existing = check_registry.fn(registry, op_id)
    if existing is not None:
        return registry, existing.result, True  # skipped
    result = execute_operation.fn(name, inputs)
    registry = register_operation.fn(registry, op_id, name, op_id, result)
    return registry, result, False  # executed
```

Duplicate operations are detected by hashing name + inputs. The registry
prevents re-execution.

---

### Error Recovery

**What it demonstrates:** Checkpoint-based stage recovery. The flow saves
progress after each stage; re-runs skip completed stages.

**Airflow equivalent:** None (production resilience combining manifest and
checkpoint ideas).

```python
@task
def run_with_checkpoints(stages, store_path, fail_on=None):
    store = load_checkpoints.fn(store_path)
    for stage in stages:
        if not should_run_stage.fn(store, stage):
            recovered += 1
            continue
        result = execute_stage.fn(stage, context, fail_on)
        store = save_checkpoint.fn(store, stage, "completed", result, store_path)
```

Fail at stage X, re-run, and stages before X are automatically skipped.

---

### Production Pipeline v3

**What it demonstrates:** Phase 4 capstone combining file I/O, data profiling,
quality rules, enrichment with caching, deduplication, and checkpointing.

**Airflow equivalent:** Quality framework + dashboard capstone (DAGs 099, 098).

```python
@flow(name="data_engineering_production_pipeline_v3", log_prints=True)
def production_pipeline_v3_flow(work_dir=None):
    records = ingest_csv(input_path)
    profile = profile_data(records)
    quality = run_quality_checks(records, rules)
    enriched, cache_stats = enrich_records(records, cache)
    deduped = deduplicate_records(enriched, ["id", "name"])
    write_output(deduped, output_path)
    build_dashboard(result)
```

This capstone combines all Phase 4 patterns: CSV file I/O, statistical
profiling, quality rule checks with traffic-light scoring, application-level
caching, hash-based deduplication, checkpoint saving, and a markdown dashboard
artifact.

---

## Environmental and Risk Analysis 

### Air Quality Index

**What it demonstrates:** Threshold-based AQI classification against WHO
air quality standards, health advisory generation, and severity ordering.

**Airflow equivalent:** Air quality monitoring with WHO thresholds (DAG 083).

```python
AQI_THRESHOLDS: list[tuple[float, str, str]] = [
    (50.0, "Good", "green"),
    (100.0, "Moderate", "yellow"),
    (150.0, "Unhealthy for Sensitive Groups", "orange"),
    (200.0, "Unhealthy", "red"),
    (300.0, "Very Unhealthy", "purple"),
    (float("inf"), "Hazardous", "maroon"),
]

@task
def classify_aqi(readings: list[PollutantReading]) -> list[AqiClassification]:
    classifications: list[AqiClassification] = []
    for reading in readings:
        mean_val = sum(reading.hourly_values) / len(reading.hourly_values)
        for threshold, cat, col in AQI_THRESHOLDS:
            if mean_val <= threshold:
                category = cat
                color = col
                break
        ...
```

Readings are classified by walking an ordered list of (threshold, category,
color) tuples. A severity ordering list determines the worst city. WHO limits
are checked separately for exceedance counting.

---

### Composite Risk Assessment

**What it demonstrates:** Multi-source weighted risk scoring combining marine
and flood data into a composite index.

**Airflow equivalent:** Marine forecast + flood discharge composite risk (DAG 084).

```python
@task
def compute_composite(
    location: str,
    marine_factors: list[RiskFactor],
    flood_factors: list[RiskFactor],
    marine_weight: float,
    flood_weight: float,
) -> CompositeRisk:
    marine_avg = sum(f.normalized_score for f in marine_factors) / len(marine_factors)
    flood_avg = sum(f.normalized_score for f in flood_factors) / len(flood_factors)
    weighted = marine_avg * marine_weight + flood_avg * flood_weight
    category = _classify_risk(weighted)
    ...
```

Raw risk factors are normalized to a 0-100 scale, then combined with
configurable weights (default 60/40 marine/flood). The composite score is
classified into low/moderate/high/critical categories.

---

### Daylight Analysis

**What it demonstrates:** Datetime arithmetic for seasonal daylight profiles
and Pearson correlation between latitude and daylight amplitude.

**Airflow equivalent:** Sunrise-sunset daylight analysis across latitudes (DAG 085).

```python
@task
def correlate_latitude_amplitude(profiles: list[SeasonalProfile]) -> float:
    x = [abs(p.latitude) for p in profiles]
    y = [p.amplitude for p in profiles]
    mean_x = statistics.mean(x)
    mean_y = statistics.mean(y)
    numerator = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y, strict=True))
    denom_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
    denom_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))
    return numerator / (denom_x * denom_y)
```

Day length is computed using a sinusoidal formula with amplitude proportional to
latitude. The manual Pearson correlation confirms the expected strong positive
relationship between absolute latitude and seasonal daylight variation.

---

### Statistical Aggregation

**What it demonstrates:** Fan-out aggregation: one dataset, three independent
aggregations (by station, by date, cross-tabulation) running in parallel.

**Airflow equivalent:** Parquet-style aggregation with groupby and cross-tab (DAG 057).

```python
# Fan-out: 3 independent aggregations
station_future = aggregate_by_group.submit(records, "station", "temperature")
date_future = aggregate_by_group.submit(records, "day", "temperature")
cross_tab_future = build_cross_tab.submit(records, "station", "day", "temperature")

station_agg = station_future.result()
date_agg = date_future.result()
cross_tab = cross_tab_future.result()
```

`.submit()` launches the three aggregations concurrently. The cross-tabulation
builds a station-by-day matrix of mean temperatures using `statistics.mean()`.

---

## Economic and Demographic Analysis 

### Demographic Analysis

**What it demonstrates:** Nested JSON normalization into relational tables,
bridge tables for multi-valued fields, and border graph edge construction.

**Airflow equivalent:** Country demographics with nested JSON normalization (DAG 087).

```python
@task
def build_bridge_table(countries: list[Country], field: str) -> list[BridgeRecord]:
    records: list[BridgeRecord] = []
    for c in countries:
        mapping = getattr(c, field)
        for key, value in mapping.items():
            records.append(BridgeRecord(country=c.name, key=key, value=value))
    return records
```

Nested JSON fields (languages, currencies) are exploded into bridge tables
linking country to key-value pairs. Border relationships become directed graph
edges. Countries are ranked by population and density.

---

### Multi-Indicator Correlation

**What it demonstrates:** Multi-indicator join on (country, year), forward-fill
for missing values, and pairwise Pearson correlation matrix.

**Airflow equivalent:** World Bank multi-indicator analysis with correlation (DAG 088).

```python
def _pearson(x: list[float], y: list[float]) -> float:
    mean_x = statistics.mean(x)
    mean_y = statistics.mean(y)
    num = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y, strict=True))
    den_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
    den_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))
    return num / (den_x * den_y)
```

Three indicator series (GDP, CO2, Renewable) are joined on country+year,
forward-filled within each country, and then tested for pairwise correlation.
Year-over-year growth rates are also computed.

---

### Financial Time Series

**What it demonstrates:** Log returns, rolling window volatility (annualized),
cross-currency correlation matrix, and anomaly detection via z-score.

**Airflow equivalent:** Currency analysis with log returns and volatility (DAG 089).

```python
@task
def compute_log_returns(records: list[RateRecord]) -> dict[str, list[LogReturn]]:
    ...
    for i in range(1, len(sorted_rates)):
        log_ret = math.log(sorted_rates[i].rate / sorted_rates[i - 1].rate)
        currency_returns.append(LogReturn(day=sorted_rates[i].day, return_value=round(log_ret, 8)))
    ...
```

Log returns are the natural logarithm of consecutive price ratios. Rolling
volatility is the standard deviation of returns over a window, annualized by
multiplying by sqrt(252). Anomalies are returns exceeding 2 standard deviations.

---

### Hypothesis Testing

**What it demonstrates:** Educational null-hypothesis pattern: align seismic
and weather datasets, test for correlation, and interpret the (expected near-zero)
result.

**Airflow equivalent:** Earthquake-weather correlation / null hypothesis (DAG 093).

```python
@task
def check_correlation(observations: list[DailyObservation], hypothesis: str) -> HypothesisResult:
    x = [o.metric_a for o in observations]
    y = [o.metric_b for o in observations]
    r_val = _pearson(x, y)
    interpretation = _interpret_result(r_val, len(observations))
    is_significant = abs(r_val) > 0.3
    ...
```

Two hypotheses are tested (earthquakes vs temperature, earthquakes vs pressure).
Both produce near-zero correlations, confirming the null hypothesis. The absence
of correlation is itself a valid finding.

---

## Advanced Analytics 

### Regression Analysis

**What it demonstrates:** Manual OLS linear regression with log transformation,
R-squared computation, and residual-based efficiency ranking.

**Airflow equivalent:** Health expenditure log-linear regression (DAG 096).

```python
@task
def linear_regression(x: list[float], y: list[float]) -> RegressionResult:
    mean_x = statistics.mean(x)
    mean_y = statistics.mean(y)
    cov_xy = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y, strict=True)) / n
    var_x = sum((xi - mean_x) ** 2 for xi in x) / n
    slope = cov_xy / var_x if var_x > 0 else 0.0
    intercept = mean_y - slope * mean_x
    ss_res = sum((yi - (slope * xi + intercept)) ** 2 for xi, yi in zip(x, y, strict=True))
    ss_tot = sum((yi - mean_y) ** 2 for yi in y)
    r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
    ...
```

Health spending is log-transformed before regression against mortality. Countries
are ranked by residual: negative residual means lower mortality than predicted
(more efficient). No numpy or scipy required.

---

### Star Schema

**What it demonstrates:** Dimensional modeling with fact and dimension tables,
surrogate keys, min-max normalization, and weighted composite index ranking.

**Airflow equivalent:** Health profile dimensional model (DAG 097).

```python
@task
def build_country_dimension(data: list[dict]) -> list[DimCountry]:
    dims: list[DimCountry] = []
    for i, d in enumerate(data, 1):
        dims.append(DimCountry(key=i, name=d["name"], region=d["region"], population=d["population"]))
    return dims
```

Three dimension tables (country, time, indicator) and a fact table form a star
schema. Indicators are min-max normalized per the `higher_is_better` flag, then
weighted to produce a composite country ranking.

---

### Staged ETL Pipeline

**What it demonstrates:** Three-layer ETL pipeline: staging (raw load with
timestamp), production (validated + transformed), and summary (grouped stats).

**Airflow equivalent:** SQL-based three-layer ETL (DAGs 035-036).

```python
@task
def validate_and_transform(staging: list[StagingRecord]) -> list[ProductionRecord]:
    for s in staging:
        is_valid = True
        try:
            value = float(s.value_raw)
            if value < 0 or value > 1000:
                is_valid = False
        except (ValueError, TypeError):
            is_valid = False
        ...
```

Each record carries an `is_valid` flag through the production layer. Invalid
records are retained but flagged. The summary layer computes grouped statistics
(avg, min, max, count) over valid records only.

---

### Data Transfer

**What it demonstrates:** Cross-system data synchronization with computed
categorical columns and transfer verification via row count and checksum.

**Airflow equivalent:** Generic table-to-table transfer with transformation (DAG 037).

```python
@task
def verify_transfer(sources: list[SourceRecord], destinations: list[DestRecord]) -> TransferVerification:
    count_match = len(sources) == len(destinations)
    source_hash = hashlib.sha256(
        "|".join(f"{s.city}:{s.population}" for s in sorted(sources, key=lambda x: x.city)).encode()
    ).hexdigest()[:16]
    dest_hash = hashlib.sha256(
        "|".join(f"{d.city}:{d.population}" for d in sorted(destinations, key=lambda x: x.city)).encode()
    ).hexdigest()[:16]
    return TransferVerification(count_match=count_match, checksum_match=source_hash == dest_hash)
```

Source records are enriched with a `size_category` during transfer. Verification
checks both row counts and SHA-256 checksums to ensure data integrity.

---

## Domain API Processing 

### Hierarchical Data Processing

**What it demonstrates:** Tree-structured org unit hierarchy with path-based
depth computation, parent field flattening, and root/leaf identification.

**Airflow equivalent:** DHIS2 org unit hierarchy flattening (DAG 058).

```python
@task
def flatten_hierarchy(raw: list[RawOrgUnit]) -> list[OrgUnit]:
    units: list[OrgUnit] = []
    for r in raw:
        parent_id = r.parent.id if r.parent else ""
        parent_name = r.parent.name if r.parent else ""
        depth = len([s for s in r.path.split("/") if s])
        units.append(OrgUnit(id=r.id, name=r.name, level=r.level,
                             parent_id=parent_id, parent_name=parent_name,
                             path=r.path, hierarchy_depth=depth, ...))
    ...
```

Nested parent references are flattened to `parent_id` and `parent_name` columns.
Hierarchy depth is derived from the path string. Root and leaf nodes are
identified by comparing parent and child ID sets.

---

### Expression Complexity Scoring

**What it demonstrates:** Regex-based expression parsing for operand counting,
operator counting, complexity scoring, and binning.

**Airflow equivalent:** DHIS2 indicator expression parsing (DAGs 059-060).

```python
OPERAND_PATTERN = re.compile(r"#\{[^}]+\}")

@task
def score_complexity(expressions: list[Expression]) -> list[ComplexityScore]:
    for expr in expressions:
        num_operands = parse_operands.fn(expr.numerator) + parse_operands.fn(expr.denominator)
        num_operators = count_operators.fn(expr.numerator) + count_operators.fn(expr.denominator)
        total = num_operands + num_operators
        if total <= 2:
            bin_label = "trivial"
        elif total <= 4:
            bin_label = "simple"
        elif total <= 8:
            bin_label = "moderate"
        else:
            bin_label = "complex"
        ...
```

Expressions use `#{...}` operand syntax. The regex counts operands while a
character scan counts operators. The combined score determines a complexity bin
(trivial/simple/moderate/complex).

---

### Spatial Data Construction

**What it demonstrates:** Manual GeoJSON-like feature collection construction,
bounding box computation from point geometries, and geometry type filtering.

**Airflow equivalent:** DHIS2 org unit geometry / GeoJSON construction (DAG 061).

```python
@task
def compute_bounding_box(features: list[Feature]) -> list[float]:
    lons: list[float] = []
    lats: list[float] = []
    for f in features:
        if f.geometry_type == "Point":
            lons.append(f.coordinates[0])
            lats.append(f.coordinates[1])
    return [min(lons), min(lats), max(lons), max(lats)]
```

Features with Point and Polygon geometry types are assembled into a collection.
The bounding box is computed from point coordinates as [min_lon, min_lat,
max_lon, max_lat].

---

### Parallel Multi-Endpoint Export

**What it demonstrates:** Parallel independent endpoint processing with
heterogeneous output formats (CSV + JSON) and fan-in summary.

**Airflow equivalent:** DHIS2 combined parallel export (DAG 062).

```python
# Fan-out: 3 parallel endpoint fetches
future_a = fetch_endpoint_a.submit(output_dir)
future_b = fetch_endpoint_b.submit(output_dir)
future_c = fetch_endpoint_c.submit(output_dir)

result_a = future_a.result()
result_b = future_b.result()
result_c = future_c.result()

summary = combine_results([result_a, result_b, result_c], duration)
```

Three endpoints are fetched in parallel via `.submit()`, writing CSV and JSON
files. Results are combined into a summary with total record count and format
distribution.

---

## Advanced Patterns and Grand Capstone 

### Data Lineage Tracking

**What it demonstrates:** Hash-based provenance tracking through pipeline
stages, building a lineage graph from ingest through filter, enrich, and dedup.

**Airflow equivalent:** None (Prefect-native pattern).

```python
@task
def compute_data_hash(records: list[DataRecord]) -> str:
    raw = str(sorted(str(r.model_dump()) for r in records))
    return hashlib.sha256(raw.encode()).hexdigest()[:16]

@task
def transform_filter(records, min_value):
    input_hash = compute_data_hash.fn(records)
    filtered = [r for r in records if r.value >= min_value]
    output_hash = compute_data_hash.fn(filtered)
    entry = record_lineage.fn("transform", "filter_by_value", input_hash, output_hash, len(filtered))
    return filtered, entry
```

Every transformation records its input and output hashes, creating a chain of
lineage entries. The lineage graph tracks how data changes through each stage
and counts data-modifying operations (where input_hash differs from output_hash).

---

### Pipeline Template Factory

**What it demonstrates:** Reusable pipeline templates with ordered stage slots,
instantiated with different configurations via the factory pattern.

**Airflow equivalent:** None (Prefect-native pattern).

```python
@task
def execute_stage(stage: StageTemplate, overrides: dict) -> StageResult:
    merged = {**stage.default_params, **overrides}
    if stage.stage_type == "extract":
        count = int(merged.get("batch_size", 100))
    elif stage.stage_type == "validate":
        count = int(int(merged.get("batch_size", 100)) * float(merged.get("pass_rate", 0.9)))
    ...
```

Templates define ordered stages with default parameters. Instantiation merges
overrides into defaults. The same template produces different pipelines depending
on the configuration, demonstrating code reuse without duplication.

---

### Multi-Pipeline Orchestrator

**What it demonstrates:** Orchestration of multiple independent mini-pipelines
with status collection and overall health rollup.

**Airflow equivalent:** None (Prefect-native pattern).

```python
@task
def aggregate_status(statuses: list[PipelineStatus]) -> OrchestratorResult:
    successful = sum(1 for s in statuses if s.status == "success")
    failed = len(statuses) - successful
    if failed == 0:
        overall = "healthy"
    elif failed < len(statuses) / 2:
        overall = "degraded"
    else:
        overall = "critical"
    ...
```

Four independent pipelines (ingest, transform, export, quality) run and report
status. The orchestrator aggregates results into healthy/degraded/critical based
on majority voting and produces a markdown report.

---

### Grand Capstone

**What it demonstrates:** End-to-end analytics pipeline combining patterns from
all 5 phases: CSV I/O, profiling, quality checks, enrichment, deduplication,
regression, dimensional modeling, lineage tracking, and a dashboard artifact.

**Airflow equivalent:** None (combines patterns from all 5 phases).

```python
@flow(name="analytics_grand_capstone", log_prints=True)
def grand_capstone_flow(work_dir: str | None = None) -> CapstoneResult:
    records = ingest_data(input_path)
    profile_data(records)
    quality = run_quality_checks(records)
    cleaned = enrich_and_deduplicate(records)
    regression = run_regression(cleaned, "value", "score")
    dimensions = build_dimensions(cleaned)
    lineage = track_lineage(stages)
    build_capstone_dashboard(result)
    ...
```

This final flow ties together every major concept from Phase 1 through Phase 5:
file ingestion, statistical profiling, quality rule checking, hash-based
deduplication, OLS regression, dimensional modeling with composite ranking,
lineage tracking, and a rich markdown dashboard artifact.

---

## DHIS2 Integration (101--108)

### DHIS2 Connection Block

**What it demonstrates:** Custom Prefect block with methods for DHIS2 API
operations, `SecretStr` for password management, connection verification.

**Airflow equivalent:** `BaseHook.get_connection("dhis2_default")` (DAG 110).

```python
class Dhis2Credentials(Block):
    base_url: str = "https://play.im.dhis2.org/dev"
    username: str = "admin"
    password: SecretStr = Field(default=SecretStr("district"))

    def get_client(self) -> Dhis2Client: ...

class Dhis2Client:
    def get_server_info(self) -> dict: ...
    def fetch_metadata(self, endpoint: str) -> list[dict]: ...

creds = get_dhis2_credentials()       # Block.load() with fallback
client = creds.get_client()
info = get_connection_info(creds)     # SecretStr handles masking
verify_connection(client, creds.base_url)  # uses client.get_server_info()
```

The Airflow pattern `BaseHook.get_connection()` maps to `Block.load()` in
Prefect. Password is stored as `SecretStr` directly on the block (encrypted
at rest when saved to the server). `get_client()` returns a `Dhis2Client`
that handles API calls -- callers never touch the password directly.

---

### DHIS2 Block Connection

**What it demonstrates:** Named credentials block loading so different DHIS2
instances (play, staging, production) can be targeted at runtime via a single
``instance`` parameter.

**Airflow equivalent:** Multiple connections by conn_id.

```python
@flow(name="dhis2_block_connection", log_prints=True)
def dhis2_block_connection_flow(instance: str = "dhis2") -> ConnectionInfo:
    creds = get_dhis2_credentials(instance)  # loads named block
    client = creds.get_client()
    info = get_connection_info(creds, instance)
    verify_connection(client, creds.base_url)
    count = fetch_org_unit_count(client)
    display_connection(info, count)
```

Save multiple ``Dhis2Credentials`` blocks (e.g. ``"dhis2"``, ``"dhis2-staging"``,
``"dhis2-prod"``) and pass the block name as the ``instance`` parameter. The
flow loads whichever block you specify, falling back to default play-server
credentials when no saved block exists.

---

### DHIS2 Org Units API

**What it demonstrates:** Block-authenticated metadata fetch, nested JSON
flattening with Pydantic, derived columns (hierarchy depth, translation count).

**Airflow equivalent:** DHIS2 org unit fetch (DAG 058).

```python
@task
def flatten_org_units(raw: list[RawOrgUnit]) -> list[FlatOrgUnit]:
    for r in raw:
        parent_id = r.parent["id"] if r.parent else ""
        depth = len([s for s in r.path.split("/") if s])
        flat.append(FlatOrgUnit(id=r.id, level=r.level,
                                parent_id=parent_id, hierarchy_depth=depth, ...))
```

Nested DHIS2 JSON (parent.id, createdBy.username) is extracted into flat
columns. Hierarchy depth is computed from path segment count.

---

### DHIS2 Data Elements API

**What it demonstrates:** Metadata categorization with block auth, boolean
derived columns, valueType/aggregationType grouping.

**Airflow equivalent:** DHIS2 data element fetch (DAG 059).

```python
@task
def flatten_data_elements(raw: list[RawDataElement]) -> list[FlatDataElement]:
    cc_id = r.categoryCombo["id"] if r.categoryCombo else ""
    flat.append(FlatDataElement(
        category_combo_id=cc_id,
        has_code=r.code is not None,
        name_length=len(r.name), ...))
```

Each data element is categorized by value type and aggregation type. Boolean
`has_code` and numeric `name_length` are derived columns.

---

### DHIS2 Indicators API

**What it demonstrates:** Expression parsing with regex, operand counting,
complexity scoring and binning.

**Airflow equivalent:** DHIS2 indicator fetch (DAG 060).

```python
OPERAND_PATTERN = re.compile(r"#\{[^}]+\}")

num_ops = len(OPERAND_PATTERN.findall(expression))
num_operators = sum(1 for c in expression if c in "+-*/")
complexity = num_ops + den_ops + num_operators
complexity_bin = "trivial" if complexity <= 1 else ...
```

DHIS2 indicator expressions use `#{uid.uid}` operands. The regex counts them,
operators are counted separately, and a combined score is binned into
trivial/simple/moderate/complex.

---

### DHIS2 Org Unit Geometry

**What it demonstrates:** GeoJSON construction with block auth, geometry
filtering, bounding box computation.

**Airflow equivalent:** DHIS2 org unit geometry export (DAG 061).

```python
@task
def build_collection(features: list[GeoFeature]) -> GeoCollection:
    all_points = [_extract_coords(f.geometry) for f in features]
    bbox = [min(lons), min(lats), max(lons), max(lats)]
    return GeoCollection(features=features, bbox=bbox)
```

Org units with geometry are converted to GeoJSON Features. A FeatureCollection
with bounding box is built and written to `.geojson`.

---

### DHIS2 Combined Export

**What it demonstrates:** Parallel multi-endpoint fetch with shared block,
fan-in summary across heterogeneous outputs.

**Airflow equivalent:** DHIS2 combined parallel export (DAG 062).

```python
client = get_dhis2_credentials().get_client()
future_ou = export_org_units.submit(client, output_dir)
future_de = export_data_elements.submit(client, output_dir)
future_ind = export_indicators.submit(client, output_dir)

report = combined_report([future_ou.result(), future_de.result(),
                          future_ind.result()])
```

A shared `Dhis2Client` is created once from the credentials block, then passed
to three parallel `.submit()` calls. The combined report tracks total records
and format distribution (CSV vs JSON).

---

### DHIS2 Analytics Query

**What it demonstrates:** Analytics API with dimension parameters, headers+rows
response parsing, query parameter construction.

**Airflow equivalent:** DHIS2 data values / analytics (DAG 111).

```python
@task
def parse_analytics(response: RawAnalyticsResponse) -> list[AnalyticsRow]:
    header_names = [h["name"] for h in response.headers]
    for row_data in response.rows:
        record = dict(zip(header_names, row_data, strict=True))
        rows.append(AnalyticsRow(dx=record["dx"], ou=record["ou"], ...))
```

The DHIS2 analytics API returns headers and rows separately. This flow builds
dimension queries, fetches results, and parses the tabular response into typed
records.

---

### DHIS2 Full Pipeline

**What it demonstrates:** End-to-end DHIS2 pipeline with block config, quality
checks, timing, and markdown dashboard.

**Airflow equivalent:** None (capstone combining all DHIS2 patterns).

```python
@flow(name="dhis2_pipeline", log_prints=True)
def dhis2_pipeline_flow() -> Dhis2PipelineResult:
    creds = get_dhis2_credentials()
    client = creds.get_client()
    connect_and_verify(client, creds.base_url)
    metadata = fetch_all_metadata(client)
    quality = validate_metadata(metadata)
    build_dashboard(result)
```

The credentials block provides a `Dhis2Client` used throughout the pipeline.
A three-stage pipeline (connect, fetch, validate) with per-stage timing,
quality scoring, and a markdown dashboard artifact. This is the DHIS2 capstone.

---

### DHIS2 World Bank Population Import

**What it demonstrates:** First *write* flow -- fetches World Bank population
data (SP.POP.TOTL) and imports it into a DHIS2 data element via
`POST /api/dataValueSets`.

**Airflow equivalent:** PythonOperator chain with World Bank fetch + DHIS2 POST.

```python
@flow(name="dhis2_worldbank_import", log_prints=True)
def dhis2_worldbank_import_flow(query: ImportQuery | None = None) -> ImportResult:
    client = get_dhis2_credentials().get_client()
    populations = fetch_population_data(query.iso3_codes, query.start_year, query.end_year)
    org_unit_map = resolve_org_units(client, query.iso3_codes)
    data_values = build_data_values(populations, org_unit_map, query.data_element_uid)
    result = import_data_values(client, data_values)
    create_markdown_artifact(key="dhis2-worldbank-import", markdown=result.markdown)
```

Four tasks form a pipeline: (1) batch-fetch population from the World Bank API,
(2) resolve ISO3 codes to DHIS2 org unit UIDs, (3) build data values matching the
DHIS2 `dataValueSets` schema, (4) POST and parse the import summary. Unresolved
ISO3 codes are logged as warnings and skipped. Retry-enabled tasks handle transient
API failures on both the World Bank and DHIS2 sides.

---

## Connection Patterns

### Environment-Based Configuration

**What it demonstrates:** Four configuration strategies: inline blocks, Secret
blocks, environment variables, JSON config. Compares all approaches in a
single flow.

**Airflow equivalent:** None (Prefect-native pattern).

```python
sources.append(from_inline_block())    # Dhis2Credentials()
sources.append(from_secret_block())    # Secret.load()
sources.append(from_env_var("KEY"))    # os.environ.get()
sources.append(from_json_config())     # JSON.load()
report = compare_strategies(sources)
```

Each strategy is loaded with graceful fallback. The comparison report shows
which strategies are available in the current environment.

---

### Authenticated API Pipeline

**What it demonstrates:** Reusable pattern for any authenticated API: API key,
bearer token, and basic auth. Generic API client block with pluggable auth.

**Airflow equivalent:** None (general pattern).

```python
class ApiAuthConfig(Block):
    auth_type: str  # "api_key", "bearer", "basic"
    base_url: str

@task
def build_auth_header(config: ApiAuthConfig, credentials: str) -> AuthHeader:
    if config.auth_type == "api_key":
        return AuthHeader(header_name="X-API-Key", ...)
    elif config.auth_type == "bearer":
        return AuthHeader(header_name="Authorization", header_value=f"Bearer ...")
    elif config.auth_type == "basic":
        encoded = base64.b64encode(credentials.encode()).decode()
        return AuthHeader(header_name="Authorization", header_value=f"Basic ...")
```

Three authentication strategies are tested against a simulated API. The block
pattern is reusable for any authenticated HTTP service.

---

## Cloud Storage

### S3 Parquet Export

**What it demonstrates:** Generate sample data as Pydantic models, transform
with pandas, and write parquet to S3-compatible storage (RustFS/MinIO) using
prefect-aws blocks.

**Airflow equivalent:** PythonOperator + S3Hook.upload_file().

```python
class SensorReading(BaseModel):
    station: StationId
    date: date
    temperature_c: float = Field(ge=-50.0, le=60.0)
    humidity_pct: float = Field(ge=0.0, le=100.0)
    status: OperationalStatus

    @computed_field
    @property
    def heat_index(self) -> float:
        return round(self.temperature_c + 0.05 * self.humidity_pct, 1)

    @computed_field
    @property
    def temp_category(self) -> TempCategory:
        if self.temperature_c <= 0.0:
            return TempCategory.COLD
        ...

@task
def transform_to_dataframe(readings: list[SensorReading]) -> TransformResult:
    rows = [r.model_dump() for r in readings]
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    return TransformResult(parquet_data=buf.getvalue(), ...)

@task
def upload_to_s3(transform: TransformResult, key: str) -> UploadResult:
    minio_creds = MinIOCredentials(minio_root_user="admin", ...)
    bucket = S3Bucket(bucket_name="prefect-data", credentials=minio_creds,
                      aws_client_parameters=AwsClientParameters(endpoint_url=...))
    bucket.upload_from_file_object(io.BytesIO(transform.parquet_data), key)
    return UploadResult(key=key, backend=StorageBackend.S3, ...)
```

Sensor readings are validated with Pydantic `Field` constraints and `computed_field`
for derived values (heat index, temperature category). The `model_dump()` method
converts validated models to dicts for pandas. `MinIOCredentials` and `S3Bucket`
from prefect-aws provide the S3 client -- the same blocks work with AWS S3 or any
S3-compatible service (MinIO, RustFS). If S3 is unavailable, the flow falls back
to a local temp file.

---

### DHIS2 GeoParquet Export

**What it demonstrates:** Fetch org units with geometry from DHIS2, build a
GeoDataFrame with shapely, and export to S3-compatible storage as GeoParquet
(OGC standard for geospatial data in Parquet format).

**Airflow equivalent:** PythonOperator + S3Hook.upload_file() with GeoPandas.

```python
@task
def build_geodataframe(org_units: list[dict[str, Any]]) -> gpd.GeoDataFrame:
    rows = []
    for ou in org_units:
        geom = ou.get("geometry")
        if not geom:
            continue
        rows.append({
            "id": ou["id"],
            "name": ou.get("name", ""),
            "geometry": shape(geom),
        })
    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")

@task
def export_geoparquet(gdf: gpd.GeoDataFrame) -> bytes:
    buf = io.BytesIO()
    gdf.to_parquet(buf, engine="pyarrow", index=False)
    return buf.getvalue()

@flow(name="cloud_dhis2_geoparquet_export", log_prints=True)
def dhis2_geoparquet_export_flow(instance: str = "dhis2") -> ExportReport:
    creds = get_dhis2_credentials(instance)
    client = creds.get_client()
    org_units = fetch_org_units_with_geometry(client)
    gdf = build_geodataframe(org_units)
    data = export_geoparquet(gdf)
    key, backend = upload_to_s3(data, s3_key)
    report = build_report(gdf, data, key, backend)
    return report
```

`shapely.geometry.shape()` converts DHIS2 GeoJSON geometry into Shapely objects.
`GeoDataFrame.to_parquet()` writes OGC GeoParquet with embedded CRS metadata.
The same S3 upload/fallback pattern from the S3 Parquet Export flow is reused.
Multi-instance support via the `instance` parameter loads different DHIS2
credentials blocks.

---

## WorldPop API

### WorldPop Dataset Catalog

**What it demonstrates:** Hierarchical REST API navigation and dataset
discovery using the WorldPop catalog API. Queries top-level datasets,
drills into sub-datasets, and filters by country ISO3 code.

**Airflow equivalent:** PythonOperator + HttpHook for REST API traversal.

```python
@task(retries=2, retry_delay_seconds=[2, 5])
def list_datasets() -> list[DatasetRecord]:
    with httpx.Client(timeout=30) as client:
        resp = client.get(WORLDPOP_CATALOG_URL)
        resp.raise_for_status()
    return [DatasetRecord(id=item.get("alias", ""), ...) for item in resp.json()["data"]]

@task(retries=2, retry_delay_seconds=[2, 5])
def query_country_datasets(dataset: str, subdataset: str, iso3: str) -> list[CountryDatasetRecord]:
    url = f"{WORLDPOP_CATALOG_URL}/{dataset}/{subdataset}"
    with httpx.Client(timeout=30) as client:
        resp = client.get(url, params={"iso3": iso3})
        resp.raise_for_status()
    return [CountryDatasetRecord(...) for item in resp.json()["data"]]

@flow(name="cloud_worldpop_dataset_catalog", log_prints=True)
def worldpop_dataset_catalog_flow(query: DatasetQuery | None = None) -> CatalogReport:
    datasets = list_datasets()
    subdatasets = list_subdatasets(query.dataset)
    country_records = query_country_datasets(query.dataset, query.subdataset, query.iso3)
    report = build_catalog_report(datasets, subdatasets, country_records, query)
    create_markdown_artifact(key="worldpop-dataset-catalog", markdown=report.markdown)
    return report
```

Multi-level REST API traversal with optional parameters at each level. Pydantic
models validate each API response shape. The flow produces a markdown artifact
with tables of available datasets, sub-datasets, and country records.

---

### WorldPop Population Stats

**What it demonstrates:** GeoJSON spatial queries against the WorldPop stats
API, with support for both synchronous and asynchronous (polling) execution
modes.

**Airflow equivalent:** PythonOperator + HttpHook with polling sensor.

```python
@task(retries=2, retry_delay_seconds=[2, 5])
def query_population(year: int, geojson: dict[str, Any], run_async: bool = False) -> PopulationResult:
    params = {"dataset": "wpgppop", "year": str(year), "geojson": json.dumps(geojson)}
    if run_async:
        params["runasync"] = "true"
    with httpx.Client(timeout=30) as client:
        resp = client.get(WORLDPOP_STATS_URL, params=params)
        resp.raise_for_status()
        body = resp.json()
    if run_async and body.get("status") == "created":
        body = _poll_async_result(body["taskid"])
    return PopulationResult(total_population=float(body["data"]["total_population"]), year=year)
```

GeoJSON polygons are passed as flow parameters. The async mode demonstrates a
polling pattern -- submit a job, then poll for results with timeout handling.

---

### WorldPop Age-Sex Pyramid

**What it demonstrates:** Demographic data transformation from the WorldPop
age-sex stats API. Parses age-sex brackets, computes dependency ratio, sex
ratio, and median age bracket.

**Airflow equivalent:** PythonOperator + HttpHook with data transformation.

```python
@task
def compute_demographics(brackets: list[AgeSexBracket]) -> DemographicStats:
    total_male = sum(b.male for b in brackets)
    total_female = sum(b.female for b in brackets)
    sex_ratio = (total_male / total_female * 100) if total_female > 0 else 0.0
    young = sum(b.total for b in brackets[:3])
    old = sum(b.total for b in brackets[13:])
    working_age = sum(b.total for b in brackets[3:13])
    dependency_ratio = ((young + old) / working_age * 100) if working_age > 0 else 0.0
    return DemographicStats(sex_ratio=round(sex_ratio, 1), dependency_ratio=round(dependency_ratio, 1), ...)
```

Complex API response parsing into typed Pydantic models. Computed statistics
(dependency ratio, sex ratio) demonstrate data transformation patterns. The
markdown artifact includes both a tabular pyramid and summary statistics.

---

### WorldPop Country Comparison

**What it demonstrates:** Parallel multi-country queries using `.map()`,
aggregation, and comparison table generation.

**Airflow equivalent:** PythonOperator in a loop or dynamic task mapping.

```python
@flow(name="cloud_worldpop_country_comparison", log_prints=True)
def worldpop_country_comparison_flow(query: ComparisonQuery | None = None) -> ComparisonReport:
    futures = fetch_country_metadata.map(
        query.iso3_codes,
        dataset=query.dataset,
        subdataset=query.subdataset,
        year=query.year,
    )
    records = [f.result() for f in futures]
    sorted_records = aggregate_comparison(records)
    report = build_comparison_report(sorted_records, query)
    create_markdown_artifact(key="worldpop-country-comparison", markdown=report.markdown)
    return report
```

`.map()` fans out one API call per country in parallel. Results are collected
and aggregated into a sorted comparison table. The flow demonstrates Prefect's
dynamic mapping pattern with real API calls.

---

### WorldPop Country Report

**What it demonstrates:** Multi-source API aggregation -- WorldPop catalog
metadata enriched with population numbers from the World Bank API -- followed
by Slack notification via webhook.

**External APIs:**
- **WorldPop catalog API** (`hub.worldpop.org/rest/data`) -- dataset metadata
  and year-range availability per country.
- **World Bank API** (`api.worldbank.org/v2`) -- country-level population by
  ISO3 code and year (indicator `SP.POP.TOTL`).

**Airflow equivalent:** PythonOperator + HttpHook + SlackWebhookOperator.

```python
@flow(name="cloud_worldpop_country_report", log_prints=True)
def worldpop_country_report_flow(query: CountryReportQuery | None = None) -> PopulationReport:
    futures = fetch_country_data.map(query.iso3_codes, dataset=query.dataset, subdataset=query.subdataset)
    raw_countries = [f.result() for f in futures]
    pop_data = fetch_population(query.iso3_codes, query.year)
    for c in raw_countries:
        c.population = pop_data.get(c.iso3, 0)
    countries = transform_results(raw_countries)
    markdown = build_report(countries)
    create_markdown_artifact(key="worldpop-country-report", markdown=markdown)
    slack_sent = send_slack_notification(countries)
    return PopulationReport(countries=countries, total_countries=len(countries), markdown=markdown, slack_sent=slack_sent)
```

The World Bank batch endpoint accepts semicolon-separated ISO3 codes in a
single request, avoiding per-country fan-out for population data. Results are
merged into the WorldPop metadata before sorting by population descending.

---

### WorldPop Population Time-Series

**What it demonstrates:** Time-series construction from sequential API queries,
year-over-year growth rate computation, and peak growth identification.

**Airflow equivalent:** PythonOperator loop with HttpHook for paginated API.

```python
@task
def compute_growth_rates(year_records: list[YearMetadata]) -> list[GrowthRate]:
    growth_rates = []
    for i in range(1, len(year_records)):
        prev_year = year_records[i - 1].year
        curr_year = year_records[i].year
        year_gap = curr_year - prev_year
        compound_rate = ((1 + 2.5 / 100) ** year_gap - 1) * 100
        growth_rates.append(GrowthRate(from_year=prev_year, to_year=curr_year, growth_rate_pct=round(compound_rate, 2)))
    return growth_rates
```

Sequential API pagination over available years for a single country. Growth
rates are computed from consecutive year metadata. The markdown artifact
includes both the year listing and a growth rate table with peak identification.

---

## World Bank API

### World Bank GDP Comparison

**What it demonstrates:** Batch multi-country API call to the World Bank
indicators endpoint, ranking and report generation.

**Airflow equivalent:** PythonOperator + HttpHook for REST API with data
ranking.

```python
@task(retries=2, retry_delay_seconds=[2, 5])
def fetch_gdp_data(iso3_codes: list[str], year: int) -> list[CountryGdp]:
    codes = ";".join(iso3_codes)
    url = f"{WORLDBANK_API_URL}/country/{codes}/indicator/NY.GDP.MKTP.CD"
    params = {"date": str(year), "format": "json", "per_page": str(len(iso3_codes))}
    with httpx.Client(timeout=30) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()
    ...

@flow(name="cloud_worldbank_gdp_comparison", log_prints=True)
def worldbank_gdp_comparison_flow(query: GdpComparisonQuery | None = None) -> GdpComparisonReport:
    countries = fetch_gdp_data(query.iso3_codes, query.year)
    ranked = rank_by_gdp(countries)
    markdown = build_gdp_report(ranked, query)
    create_markdown_artifact(key="worldbank-gdp-comparison", markdown=markdown)
    return GdpComparisonReport(countries=ranked, total_countries=len(ranked), markdown=markdown)
```

The World Bank batch endpoint accepts semicolon-separated ISO3 codes, avoiding
per-country fan-out. GDP values are ranked descending with summary statistics.

---

### World Bank Indicator Time-Series

**What it demonstrates:** Time-series construction from a single World Bank
indicator over a year range, with year-over-year growth rate computation and
peak/trough identification.

**Airflow equivalent:** PythonOperator loop with HttpHook for paginated API.

```python
@task(retries=2, retry_delay_seconds=[2, 5])
def fetch_indicator_timeseries(iso3: str, indicator: str, start_year: int, end_year: int) -> list[YearValue]:
    url = f"{WORLDBANK_API_URL}/country/{iso3}/indicator/{indicator}"
    params = {"date": f"{start_year}:{end_year}", "format": "json", "per_page": "100"}
    ...

@task
def compute_growth_rates(data: list[YearValue]) -> list[GrowthRate]:
    rates = []
    for i in range(1, len(data)):
        growth = (data[i].value - data[i-1].value) / data[i-1].value * 100
        rates.append(GrowthRate(year=data[i].year, value=data[i].value, growth_pct=round(growth, 4)))
    return rates
```

Year-range queries use `date=2000:2023` syntax. Null values from the API are
filtered before growth rate computation. Peak and trough growth years are
identified in the report summary.

---

### World Bank Country Profile

**What it demonstrates:** Multi-indicator aggregation using `.map()` to query
several different World Bank indicators for a single country in parallel.
Builds a dashboard-style country profile.

**Airflow equivalent:** PythonOperator with multiple HttpHook calls.

```python
DEFAULT_INDICATORS = [
    "SP.POP.TOTL",      # Population
    "NY.GDP.MKTP.CD",   # GDP (current US$)
    "NY.GDP.PCAP.CD",   # GDP per capita
    "SP.DYN.LE00.IN",   # Life expectancy
    "SE.ADT.LITR.ZS",   # Literacy rate
    "SH.DYN.MORT",      # Under-5 mortality rate
]

@flow(name="cloud_worldbank_country_profile", log_prints=True)
def worldbank_country_profile_flow(query: ProfileQuery | None = None) -> CountryProfile:
    futures = fetch_indicator.map(query.iso3, query.indicators, year=query.year)
    indicators = [f.result() for f in futures]
    profile = build_country_profile(query.iso3, query.year, indicators)
    create_markdown_artifact(key="worldbank-country-profile", markdown=profile.markdown)
    return profile
```

`.map()` parallelises one API call per indicator. Value formatting adapts to
indicator type: percentages for rate indicators (`.ZS` suffix), dollar
formatting for large monetary values, and comma-separated integers for counts.

---

### World Bank Poverty Analysis

**What it demonstrates:** Handling sparse/missing data common with development
indicators. Uses `.map()` for parallel country queries, trend detection, and
robust null-value handling.

**Airflow equivalent:** PythonOperator in a loop with missing-data handling.

```python
@task(retries=2, retry_delay_seconds=[2, 5])
def fetch_poverty_data(iso3: str, start_year: int, end_year: int) -> CountryPoverty:
    url = f"{WORLDBANK_API_URL}/country/{iso3}/indicator/SI.POV.DDAY"
    ...
    # Filter null values, find latest data point, determine trend
    if len(data) >= 2:
        diff = latest_value - earliest_value
        trend = "improving" if diff < -1 else "worsening" if diff > 1 else "stable"
```

Poverty data (`SI.POV.DDAY` -- headcount ratio at $2.15/day) is notoriously
sparse, with many countries missing recent data points. The flow demonstrates
robust handling of null API values, trend detection from available data, and
sorting that separates countries with data from those without.

---

## Deployments directory

Production deployment examples live in the `deployments/` directory. Each
subdirectory is a self-contained deployment with its own `flow.py`,
`prefect.yaml`, and `deploy.py`.

### dhis2_connection -- DHIS2 Connection Check

**What it demonstrates:** Verifies DHIS2 connectivity, fetches the org unit
count, and produces a connection status artifact.

```python
@flow(name="dhis2_connection", log_prints=True)
def dhis2_connection_flow() -> ConnectionReport:
    creds = get_dhis2_credentials()
    client = creds.get_client()
    server_info = verify_connection(client)
    count = fetch_org_unit_count(client)
    report = build_report(creds, server_info, count)
    return report
```

### dhis2_block_connection -- DHIS2 Block Connection

**What it demonstrates:** Verifies DHIS2 connectivity using a named credentials
block, fetches the org unit count, and produces a connection status artifact.
Supports multi-instance deployment via the `instance` parameter.

```python
@flow(name="dhis2_block_connection", log_prints=True)
def dhis2_block_connection_flow(instance: str = "dhis2") -> ConnectionReport:
    creds = get_dhis2_credentials(instance)
    client = creds.get_client()
    server_info = verify_connection(client)
    count = fetch_org_unit_count(client)
    report = build_report(creds, instance, server_info, count)
    return report
```

### dhis2_ou -- DHIS2 Organisation Units

**What it demonstrates:** A deployment-ready flow that fetches DHIS2
organisation units and produces a markdown artifact listing each unit's
ID and display name.

```python
@flow(name="dhis2_ou", log_prints=True)
def dhis2_ou_flow() -> OrgUnitReport:
    client = get_dhis2_credentials().get_client()
    org_units = fetch_org_units(client)
    report = build_report(org_units)
    return report
```

Key patterns demonstrated:

- **`prefect.runtime`** -- `deployment.name` provides deployment-aware
  context (falls back to "local" for direct runs)
- **Block reference** -- `get_dhis2_credentials()` loads credentials from a
  saved block or falls back to inline defaults
- **Markdown artifact** -- `create_markdown_artifact` produces a table
  visible in the Prefect UI artifacts tab
- **Per-deployment `prefect.yaml`** -- declarative config lives alongside
  the flow code

Deploy with `prefect.yaml`:

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

Manage after deployment:

```bash
cd deployments/dhis2_ou && prefect deploy --all   # register deployment
prefect deployment run dhis2_ou/dhis2-ou           # trigger run
prefect deployment set-schedule <name> --cron "0 8 * * *"     # change schedule
prefect deployment pause <name>                   # pause scheduling
prefect deployment resume <name>                  # resume scheduling
```

### s3_parquet_export -- S3 Parquet Export

**What it demonstrates:** Deployment wrapper for the S3 Parquet Export flow
that generates sample sensor data, transforms with pandas, and writes parquet
to S3-compatible storage.

```python
@flow(name="s3_parquet_export", log_prints=True)
def s3_parquet_export_flow(n_records: int = 500) -> ExportResult:
    readings = generate_records(n_records)
    transform = transform_to_dataframe(readings)
    upload = upload_to_s3(transform, s3_key)
    result = verify_upload(upload, transform)
    return result
```

### dhis2_geoparquet_export -- DHIS2 GeoParquet Export

**What it demonstrates:** A deployment-ready flow that fetches DHIS2 org units
with geometry, builds a GeoDataFrame, and exports GeoParquet to S3. Supports
multi-instance deployment via the `instance` parameter.

```python
@flow(name="dhis2_geoparquet_export", log_prints=True)
def dhis2_geoparquet_export_flow(instance: str = "dhis2") -> ExportReport:
    creds = get_dhis2_credentials(instance)
    client = creds.get_client()
    org_units = fetch_org_units_with_geometry(client)
    gdf = build_geodataframe(org_units)
    data = export_geoparquet(gdf)
    key, backend = upload_to_s3(data, s3_key)
    report = build_report(gdf, data, key, backend)
    return report
```
