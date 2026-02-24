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

This project focuses on local execution, but any flow can be promoted to a
deployment without changing the flow code itself.

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
