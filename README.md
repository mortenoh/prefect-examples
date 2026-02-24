# Prefect Examples

Progressive Prefect 3 examples -- from hello-world to production patterns.

Each flow is self-contained, numbered for incremental learning, and runnable
with a single command. If you are migrating from Airflow, every example notes
the equivalent Airflow concept so you can map your existing knowledge.

## Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager
- Docker and Docker Compose (optional -- for the full server stack)

## Quick start

```bash
git clone https://github.com/mortenoh/prefect-examples.git
cd prefect-examples

make sync                                  # install dependencies
uv run python flows/001_hello_world.py     # run your first flow
make test                                  # run the test suite
make server                                # start Prefect UI (http://127.0.0.1:4200)
make docs                                  # serve documentation locally
```

## Project structure

```
flows/                  110 numbered example flows
src/prefect_examples/   Shared task library
packages/prefect-dhis2/ DHIS2 integration package (workspace dependency)
deployments/            Prefect deployment definitions
tests/                  Test suite (one file per flow)
docs/                   MkDocs documentation source
compose.yml             Docker stack (PostgreSQL, Prefect server, worker)
```

## Flows

Flows are grouped into phases:

- **001--040** -- Core Prefect concepts (tasks, flows, retries, caching, deployments)
- **041--060** -- Intermediate patterns (Pydantic, factories, error handling, testing)
- **061--080** -- Data engineering (file I/O, APIs, config-driven pipelines, idempotency)
- **081--100** -- Analytics and capstone (statistics, dimensional modeling, lineage)
- **101--110** -- DHIS2 integration (custom blocks, metadata, analytics API)

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
| 061 | CSV File Processing | stdlib `csv`, `DictReader`/`DictWriter`, file-based pipeline |
| 062 | JSON Event Ingestion | Recursive JSON flattening, NDJSON output |
| 063 | Multi-File Batch | File-type dispatch, column harmonisation, hash dedup |
| 064 | Incremental Processing | Manifest-based processing, only-process-new pattern |
| 065 | Quality Rules Engine | Config-driven quality rules, traffic-light scoring |
| 066 | Cross-Dataset Validation | Referential integrity, foreign key checks |
| 067 | Data Profiling | Statistical profiling with `statistics` module |
| 068 | Pipeline Health Monitor | Meta-monitoring, file freshness, watchdog pattern |
| 069 | Multi-Source Forecast | Chained `.map()`, simulated multi-step API |
| 070 | API Pagination | Paginated API, chunked parallel processing |
| 071 | Cross-Source Enrichment | Multi-source join, partial enrichment fallback |
| 072 | Response Caching | Application-level cache, TTL, hit/miss tracking |
| 073 | Config-Driven Pipeline | Dynamic stage selection, parameter overrides |
| 074 | Producer-Consumer | File-based data contracts, separate producer/consumer |
| 075 | Circuit Breaker | State machine: closed/open/half_open, fail-fast |
| 076 | Discriminated Unions | Pydantic discriminated unions, polymorphic dispatch |
| 077 | Streaming Batch | Windowed processing, anomaly detection, trend analysis |
| 078 | Idempotent Operations | Hash-based idempotency registry, safe re-runs |
| 079 | Error Recovery | Checkpoint-based stage recovery, resume from failure |
| 080 | Production Pipeline v3 | Capstone: file I/O, profiling, quality, caching, checkpoints |
| 081 | Air Quality Index | WHO threshold classification, health advisories, severity ordering |
| 082 | Composite Risk | Multi-source weighted risk scoring, composite index |
| 083 | Daylight Analysis | Datetime arithmetic, seasonal amplitude, latitude correlation |
| 084 | Statistical Aggregation | Fan-out aggregation, grouped statistics, cross-tabulation |
| 085 | Demographic Analysis | Nested JSON normalization, bridge tables, border graph edges |
| 086 | Multi-Indicator Correlation | Multi-indicator join, forward-fill, Pearson correlation |
| 087 | Financial Time Series | Log returns, rolling volatility, anomaly detection |
| 088 | Hypothesis Testing | Cross-domain hypothesis testing, null hypothesis validation |
| 089 | Regression Analysis | Log-linear regression, R-squared, residual ranking |
| 090 | Star Schema | Dimensional modeling, surrogate keys, composite index |
| 091 | Staged ETL Pipeline | Simulated SQL ETL, staging/production/summary layers |
| 092 | Data Transfer | Cross-system data sync, computed categories, verification |
| 093 | Hierarchical Data | Tree hierarchy, path-based depth, parent flattening |
| 094 | Expression Scoring | Regex expression parsing, complexity scoring, binning |
| 095 | Spatial Data | GeoJSON-like construction, bounding box, geometry filtering |
| 096 | Parallel Export | Multi-endpoint parallel export, fan-in summary |
| 097 | Data Lineage | Provenance tracking, hash-based lineage graph |
| 098 | Pipeline Template | Reusable templates, factory pattern, config overrides |
| 099 | Multi-Pipeline Orchestrator | Independent pipeline orchestration, status rollup |
| 100 | Grand Capstone | End-to-end analytics: all Phase 5 patterns combined |
| 101 | DHIS2 Connection | Custom `Block` for DHIS2 credentials, `Secret` for password |
| 102 | DHIS2 Org Units | Block-authenticated metadata fetch, nested JSON flattening |
| 103 | DHIS2 Data Elements | Data element fetch with categorization, code coverage |
| 104 | DHIS2 Indicators | Expression parsing with regex, complexity scoring, binning |
| 105 | DHIS2 Geometry | GeoJSON construction, geometry filtering, bounding box |
| 106 | DHIS2 Combined Export | Parallel multi-endpoint export with shared block |
| 107 | DHIS2 Analytics | Analytics API query, dimension parameters, headers+rows parsing |
| 108 | DHIS2 Pipeline | End-to-end DHIS2 pipeline with quality checks and dashboard |
| 109 | DHIS2 Env Config | Environment-based configuration strategies |
| 110 | DHIS2 Authenticated API | Reusable authenticated API pattern (api_key, bearer, basic) |

## Makefile targets

| Target | Description |
|---|---|
| `make help` | Show all available targets |
| `make sync` | Install dependencies (`uv sync`) |
| `make lint` | Run ruff and mypy on `src/` and `packages/` |
| `make fmt` | Auto-format with ruff |
| `make test` | Run the test suite |
| `make run` | Run the hello-world flow |
| `make server` | Start a local Prefect server |
| `make start` | Start the full Docker stack |
| `make restart` | Tear down and rebuild the Docker stack |
| `make deploy` | Register all deployments with the server |
| `make register-blocks` | Register custom block types (DHIS2) |
| `make docs` | Serve documentation locally |
| `make docs-build` | Build static documentation site |
| `make clean` | Remove build artifacts |

## Documentation

Full documentation is available locally via MkDocs:

```bash
make docs        # http://127.0.0.1:8000
make docs-build  # build static site
```

Covers getting started, core concepts, flow reference, patterns,
infrastructure, CLI reference, API reference, and testing.

## Links

- [Prefect documentation](https://docs.prefect.io)
- [Prefect GitHub](https://github.com/PrefectHQ/prefect)
