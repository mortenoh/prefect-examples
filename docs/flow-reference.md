# Flow Reference

Detailed walkthrough of all 20 example flows, organised by category.

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

This is the capstone flow, demonstrating how subflows, mapped tasks, result
passing, and post-pipeline notifications compose into a realistic data
pipeline.
