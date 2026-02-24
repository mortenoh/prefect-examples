# Prefect Examples

Progressive Prefect 3 examples -- from hello-world to production patterns.

Each flow is self-contained, numbered for incremental learning, and runnable
with a single command. If you are migrating from Airflow, every example notes
the equivalent Airflow concept so you can map your existing knowledge.

## Quick start

```bash
git clone https://github.com/morteoh/prefect-examples.git
cd prefect-examples

make sync                                  # install dependencies
uv run python flows/001_hello_world.py     # run your first flow
make test                                  # run the test suite
make server                                # start Prefect UI (http://127.0.0.1:4200)
make docs                                  # serve documentation locally
```

## Flows

| Flow | Name | Concepts |
|------|------|----------|
| 001 | Hello World | `@flow`, `@task`, sequential execution |
| 002 | Python Tasks | Typed parameters, return values |
| 003 | Task Dependencies | `.submit()`, parallel fan-out, join |
| 004 | Taskflow ETL | Extract-transform-load, data passing |
| 005 | Task Results | Structured return values (replaces XCom) |
| 006 | Conditional Logic | Python `if/elif/else` branching |
| 007 | State Handlers | `on_failure`, `allow_failure` |
| 008 | Parameterized Flows | Typed flow parameters with defaults |
| 009 | Subflows | `@flow` calling `@flow`, nested runs |
| 010 | Dynamic Tasks | `.map()` for dynamic fan-out |
| 011 | Polling Tasks | While-loop polling, `time.sleep()` |
| 012 | Retries and Hooks | `retries`, `retry_delay_seconds`, lifecycle hooks |
| 013 | Reusable Tasks | Shared task library, Python imports |
| 014 | Events | `emit_event()`, custom observability |
| 015 | Flow of Flows | Subflow orchestration, `run_deployment()` |
| 016 | Concurrency Limits | `concurrency()` context manager, throttling |
| 017 | Variables and Params | `Variable.get()`/`set()`, runtime config |
| 018 | Early Return | Short-circuit with `return` |
| 019 | Context Managers | `try/finally`, resource setup/teardown |
| 020 | Complex Pipeline | Subflows, `.map()`, notifications, end-to-end |
| 021 | Task Caching | `cache_policy`, `INPUTS`, `TASK_SOURCE`, `cache_key_fn` |
| 022 | Task Timeouts | `timeout_seconds` on tasks and flows |
| 023 | Task Run Names | `task_run_name` template strings and callables |
| 024 | Advanced Retries | `retry_delay_seconds` list, `retry_jitter_factor`, `retry_condition_fn` |
| 025 | Structured Logging | `get_run_logger()`, `log_prints`, extra context |
| 026 | Tags | `tags=` on decorators, `tags()` context manager |
| 027 | Flow Run Names | `flow_run_name` template strings and callables |
| 028 | Result Persistence | `persist_result`, `result_storage_key` |
| 029 | Markdown Artifacts | `create_markdown_artifact()` |
| 030 | Table and Link Artifacts | `create_table_artifact()`, `create_link_artifact()` |
| 031 | Secret Block | `Secret.load()`, graceful fallback |
| 032 | Custom Blocks | Subclass `Block` for typed configuration |
| 033 | Async Tasks | `async def` tasks and flows, `await` |
| 034 | Concurrent Async | `asyncio.gather()` for parallel async tasks |
| 035 | Async Flow Patterns | Mixing sync/async tasks, async subflows |
| 036 | Async Map and Submit | `.map()` and `.submit()` with async tasks |
| 037 | Flow Serve | `flow.serve()`, cron/interval schedules |
| 038 | Schedules | `CronSchedule`, `IntervalSchedule`, `RRuleSchedule` |
| 039 | Work Pools | `flow.deploy()`, work pools, workers |
| 040 | Production Pipeline | Capstone: caching, retries, artifacts, tags, persistence |
| 041 | Pydantic Models | `BaseModel` as task params/returns, type-safe data passing |
| 042 | Shell Tasks | `subprocess.run()` in tasks (replaces BashOperator) |
| 043 | HTTP Tasks | `httpx` GET/POST in tasks (replaces HttpOperator) |
| 044 | Task Factories | Factory functions that generate `@task` callables |
| 045 | Advanced Map Patterns | Multi-arg `.map()`, chained maps, result collection |
| 046 | Error Handling ETL | Quarantine pattern: good rows pass, bad rows captured |
| 047 | Pydantic Validation | `field_validator` for data quality checks |
| 048 | SLA Monitoring | Task duration tracking, threshold comparison |
| 049 | Webhook Notifications | httpx POST notifications, flow hooks |
| 050 | Failure Escalation | Progressive retry with escalation hooks |
| 051 | Testable Flow Patterns | Pure functions + thin `@task` wrappers |
| 052 | Reusable Utilities | Custom decorators: `timed_task`, `validated_task` |
| 053 | Advanced State Handling | `allow_failure`, state inspection |
| 054 | Nested Subflows | Hierarchical `@flow` groups (replaces TaskGroups) |
| 055 | Backfill Patterns | Date-range parameters, gap detection, incremental processing |
| 056 | Runtime Context | `prefect.runtime` for flow/task metadata |
| 057 | Transactions | `transaction()` for atomic task groups |
| 058 | Interactive Flows | Human-in-the-loop approval pattern |
| 059 | Task Runners | `ThreadPoolTaskRunner`, I/O vs CPU workloads |
| 060 | Production Pipeline v2 | Capstone: Pydantic, transactions, artifacts, hooks, `.map()` |

## Documentation

Full documentation is available locally via MkDocs:

```bash
make docs        # http://127.0.0.1:8000
make docs-build  # build static site
```

## Links

- [Prefect documentation](https://docs.prefect.io)
- [Prefect GitHub](https://github.com/PrefectHQ/prefect)
