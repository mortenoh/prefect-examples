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

## Documentation

Full documentation is available locally via MkDocs:

```bash
make docs        # http://127.0.0.1:8000
make docs-build  # build static site
```

## Links

- [Prefect documentation](https://docs.prefect.io)
- [Prefect GitHub](https://github.com/PrefectHQ/prefect)
