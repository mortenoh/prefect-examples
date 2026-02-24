# Patterns

Common patterns used throughout the examples.

## Subflows for composition

Break large pipelines into smaller, independently testable flows. A parent flow
calls child flows like regular functions:

```python
@flow
def extract() -> list[dict]:
    return fetch_records()

@flow
def transform(raw: list[dict]) -> list[dict]:
    return [clean(r) for r in raw]

@flow
def pipeline():
    raw = extract()
    transformed = transform(raw)
    load(transformed)
```

Each subflow gets its own flow run in the Prefect UI, with independent state
tracking and retry behaviour.

**See:** [009 Subflows](flow-reference.md#009-subflows),
[015 Flow of Flows](flow-reference.md#015-flow-of-flows),
[020 Complex Pipeline](flow-reference.md#020-complex-pipeline)

## Dynamic mapping with `.map()`

Fan out work over a list of items at runtime. The number of items does not need
to be known at definition time:

```python
@task
def process_item(item: str) -> str:
    return f"processed({item})"

@flow
def pipeline():
    items = generate_items()          # returns ["a", "b", "c", ...]
    processed = process_item.map(items)
    summarize(processed)
```

`.map()` creates one task run per item. Results are collected as a list of
futures.

You can chain `.map()` calls to build multi-step fan-out pipelines:

```python
validated = validate_record.map(raw)
enriched = enrich_record.map(validated)
```

**See:** [010 Dynamic Tasks](flow-reference.md#010-dynamic-tasks),
[020 Complex Pipeline](flow-reference.md#020-complex-pipeline)

## Error handling with `allow_failure` and retries

### Retries

Add `retries` and `retry_delay_seconds` to any task:

```python
@task(retries=3, retry_delay_seconds=10)
def flaky_api_call() -> dict:
    ...
```

Prefect automatically retries the task up to the specified number of times,
with a delay between attempts.

### allow_failure

When a task may fail but downstream work should still run, wrap its future with
`allow_failure`:

```python
from prefect import allow_failure

@flow
def pipeline():
    risky = risky_task.submit()
    cleanup_task(wait_for=[allow_failure(risky)])
```

**See:** [007 State Handlers](flow-reference.md#007-state-handlers),
[012 Retries and Hooks](flow-reference.md#012-retries-and-hooks)

## Polling with while loops

Replace Airflow sensors with a simple polling loop inside a task:

```python
@task
def wait_for_file(path: str, interval: float = 5.0, timeout: float = 300.0) -> str:
    start = time.monotonic()
    while True:
        if Path(path).exists():
            return path
        if time.monotonic() - start > timeout:
            raise TimeoutError(f"Timed out waiting for {path}")
        time.sleep(interval)
```

Use `.submit()` to run multiple polls in parallel:

```python
future_a = poll_condition.submit(name="sensor-A")
future_b = poll_condition.submit(name="sensor-B")
process([future_a.result(), future_b.result()])
```

**See:** [011 Polling Tasks](flow-reference.md#011-polling-tasks)

## Concurrency limits

Throttle how many tasks run simultaneously using the `concurrency()` context
manager:

```python
from prefect.concurrency.sync import concurrency

@task
def rate_limited_call(item: str) -> str:
    with concurrency("api-limit", occupy=1):
        return call_external_api(item)
```

The limit name is global -- all tasks sharing the same name compete for the
same slots.

**See:** [016 Concurrency Limits](flow-reference.md#016-concurrency-limits)

## Variables and configuration

Store runtime configuration in Prefect Variables:

```python
from prefect.variables import Variable

# Write
Variable.set("my_config", '{"batch_size": 100}', overwrite=True)

# Read
raw = Variable.get("my_config", default="{}")
config = json.loads(raw)
```

Combine with typed flow parameters for flexible, environment-specific
behaviour:

```python
@flow
def pipeline(environment: str = "dev"):
    config = read_config()
    process(config, environment)
```

**See:** [017 Variables and Params](flow-reference.md#017-variables-and-params)

## State hooks for observability

Attach hook functions to tasks or flows to react to state transitions:

```python
def on_failure(task, task_run, state):
    send_alert(f"Task {task_run.name} failed: {state}")

@task(on_failure=[on_failure])
def important_task():
    ...

def on_completion(flow, flow_run, state):
    log_metric(f"Flow {flow_run.name} finished: {state.name}")

@flow(on_completion=[on_completion])
def pipeline():
    ...
```

Hooks are plain functions, not tasks. They receive the task/flow object, the
run metadata, and the final state.

**See:** [007 State Handlers](flow-reference.md#007-state-handlers),
[012 Retries and Hooks](flow-reference.md#012-retries-and-hooks)
