# Prefect Examples

Progressive Prefect 3 examples -- from hello-world to production patterns.

Each flow is self-contained, grouped by topic, and runnable with a single
command. If you are migrating from Airflow, every example notes the equivalent
Airflow concept so you can map your existing knowledge.

## Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager
- Docker and Docker Compose (optional -- for the full server stack)

## Quick start

```bash
git clone https://github.com/mortenoh/prefect-examples.git
cd prefect-examples

make sync                                  # install dependencies
uv run python flows/basics/basics_hello_world.py  # run your first flow
make test                                  # run the test suite
make server                                # start Prefect UI (http://127.0.0.1:4200)
make docs                                  # serve documentation locally
```

## Project structure

```
flows/
  basics/               20 flows -- core Prefect concepts
  core/                 20 flows -- caching, async, blocks, deployments
  patterns/             20 flows -- Pydantic, factories, error handling
  data_engineering/     20 flows -- file I/O, APIs, config-driven pipelines
  analytics/            20 flows -- statistics, dimensional modeling, lineage
  dhis2/                11 flows -- DHIS2 integration
  cloud/                 2 flows -- S3/cloud patterns
src/prefect_examples/   Shared task library
packages/prefect-dhis2/ DHIS2 integration package (workspace dependency)
deployments/            Prefect deployment definitions
scripts/                Operational scripts (block creation, setup)
tests/                  Test suite (mirrors flows/ structure)
docs/                   MkDocs documentation source
compose.yml             Docker stack (PostgreSQL, Prefect server, worker)
```

## Flows

Flows are organized into topic groups:

- **basics/** -- Core Prefect concepts (tasks, flows, retries, caching, subflows)
- **core/** -- Advanced core features (async, blocks, deployments, artifacts)
- **patterns/** -- Intermediate patterns (Pydantic, factories, error handling, testing)
- **data_engineering/** -- Data engineering (file I/O, APIs, config-driven pipelines, idempotency)
- **analytics/** -- Analytics and capstone (statistics, dimensional modeling, lineage)
- **dhis2/** -- DHIS2 integration (custom blocks, metadata, analytics API)
- **cloud/** -- Cloud patterns (S3, parquet)

| Group | Name | Concepts |
|-------|------|----------|
| basics | Hello World | `@flow`, `@task`, sequential execution |
| basics | Python Tasks | Typed parameters, return values |
| basics | Task Dependencies | `.submit()`, parallel fan-out, join |
| basics | Taskflow ETL | Extract-transform-load, data passing |
| basics | Task Results | Structured return values (replaces XCom) |
| basics | Conditional Logic | Python `if/elif/else` branching |
| basics | State Handlers | `on_failure`, `allow_failure` |
| basics | Parameterized Flows | Typed flow parameters with defaults |
| basics | Subflows | `@flow` calling `@flow`, nested runs |
| basics | Dynamic Tasks | `.map()` for dynamic fan-out |
| basics | Polling Tasks | While-loop polling, `time.sleep()` |
| basics | Retries and Hooks | `retries`, `retry_delay_seconds`, lifecycle hooks |
| basics | Reusable Tasks | Shared task library, Python imports |
| basics | Events | `emit_event()`, custom observability |
| basics | Flow of Flows | Subflow orchestration, `run_deployment()` |
| basics | Concurrency Limits | `concurrency()` context manager, throttling |
| basics | Variables and Params | `Variable.get()`/`set()`, runtime config |
| basics | Early Return | Short-circuit with `return` |
| basics | Context Managers | `try/finally`, resource setup/teardown |
| basics | Complex Pipeline | Subflows, `.map()`, notifications, end-to-end |
| core | Task Caching | `cache_policy`, `INPUTS`, `TASK_SOURCE`, `cache_key_fn` |
| core | Task Timeouts | `timeout_seconds` on tasks and flows |
| core | Task Run Names | `task_run_name` template strings and callables |
| core | Advanced Retries | `retry_delay_seconds` list, `retry_jitter_factor`, `retry_condition_fn` |
| core | Structured Logging | `get_run_logger()`, `log_prints`, extra context |
| core | Tags | `tags=` on decorators, `tags()` context manager |
| core | Flow Run Names | `flow_run_name` template strings and callables |
| core | Result Persistence | `persist_result`, `result_storage_key` |
| core | Markdown Artifacts | `create_markdown_artifact()` |
| core | Table and Link Artifacts | `create_table_artifact()`, `create_link_artifact()` |
| core | Secret Block | `Secret.load()`, graceful fallback |
| core | Custom Blocks | Subclass `Block` for typed configuration |
| core | Async Tasks | `async def` tasks and flows, `await` |
| core | Concurrent Async | `asyncio.gather()` for parallel async tasks |
| core | Async Flow Patterns | Mixing sync/async tasks, async subflows |
| core | Async Map and Submit | `.map()` and `.submit()` with async tasks |
| core | Flow Serve | `flow.serve()`, cron/interval schedules |
| core | Schedules | `CronSchedule`, `IntervalSchedule`, `RRuleSchedule` |
| core | Work Pools | `flow.deploy()`, work pools, workers |
| core | Production Pipeline | Capstone: caching, retries, artifacts, tags, persistence |
| patterns | Pydantic Models | `BaseModel` as task params/returns, type-safe data passing |
| patterns | Shell Tasks | `subprocess.run()` in tasks (replaces BashOperator) |
| patterns | HTTP Tasks | `httpx` GET/POST in tasks (replaces HttpOperator) |
| patterns | Task Factories | Factory functions that generate `@task` callables |
| patterns | Advanced Map Patterns | Multi-arg `.map()`, chained maps, result collection |
| patterns | Error Handling ETL | Quarantine pattern: good rows pass, bad rows captured |
| patterns | Pydantic Validation | `field_validator` for data quality checks |
| patterns | SLA Monitoring | Task duration tracking, threshold comparison |
| patterns | Webhook Notifications | httpx POST notifications, flow hooks |
| patterns | Failure Escalation | Progressive retry with escalation hooks |
| patterns | Testable Flow Patterns | Pure functions + thin `@task` wrappers |
| patterns | Reusable Utilities | Custom decorators: `timed_task`, `validated_task` |
| patterns | Advanced State Handling | `allow_failure`, state inspection |
| patterns | Nested Subflows | Hierarchical `@flow` groups (replaces TaskGroups) |
| patterns | Backfill Patterns | Date-range parameters, gap detection, incremental processing |
| patterns | Runtime Context | `prefect.runtime` for flow/task metadata |
| patterns | Transactions | `transaction()` for atomic task groups |
| patterns | Interactive Flows | Human-in-the-loop approval pattern |
| patterns | Task Runners | `ThreadPoolTaskRunner`, I/O vs CPU workloads |
| patterns | Production Pipeline v2 | Capstone: Pydantic, transactions, artifacts, hooks, `.map()` |
| data_engineering | CSV File Processing | stdlib `csv`, `DictReader`/`DictWriter`, file-based pipeline |
| data_engineering | JSON Event Ingestion | Recursive JSON flattening, NDJSON output |
| data_engineering | Multi-File Batch | File-type dispatch, column harmonisation, hash dedup |
| data_engineering | Incremental Processing | Manifest-based processing, only-process-new pattern |
| data_engineering | Quality Rules Engine | Config-driven quality rules, traffic-light scoring |
| data_engineering | Cross-Dataset Validation | Referential integrity, foreign key checks |
| data_engineering | Data Profiling | Statistical profiling with `statistics` module |
| data_engineering | Pipeline Health Monitor | Meta-monitoring, file freshness, watchdog pattern |
| data_engineering | Multi-Source Forecast | Chained `.map()`, simulated multi-step API |
| data_engineering | API Pagination | Paginated API, chunked parallel processing |
| data_engineering | Cross-Source Enrichment | Multi-source join, partial enrichment fallback |
| data_engineering | Response Caching | Application-level cache, TTL, hit/miss tracking |
| data_engineering | Config-Driven Pipeline | Dynamic stage selection, parameter overrides |
| data_engineering | Producer-Consumer | File-based data contracts, separate producer/consumer |
| data_engineering | Circuit Breaker | State machine: closed/open/half_open, fail-fast |
| data_engineering | Discriminated Unions | Pydantic discriminated unions, polymorphic dispatch |
| data_engineering | Streaming Batch | Windowed processing, anomaly detection, trend analysis |
| data_engineering | Idempotent Operations | Hash-based idempotency registry, safe re-runs |
| data_engineering | Error Recovery | Checkpoint-based stage recovery, resume from failure |
| data_engineering | Production Pipeline v3 | Capstone: file I/O, profiling, quality, caching, checkpoints |
| analytics | Air Quality Index | WHO threshold classification, health advisories, severity ordering |
| analytics | Composite Risk | Multi-source weighted risk scoring, composite index |
| analytics | Daylight Analysis | Datetime arithmetic, seasonal amplitude, latitude correlation |
| analytics | Statistical Aggregation | Fan-out aggregation, grouped statistics, cross-tabulation |
| analytics | Demographic Analysis | Nested JSON normalization, bridge tables, border graph edges |
| analytics | Multi-Indicator Correlation | Multi-indicator join, forward-fill, Pearson correlation |
| analytics | Financial Time Series | Log returns, rolling volatility, anomaly detection |
| analytics | Hypothesis Testing | Cross-domain hypothesis testing, null hypothesis validation |
| analytics | Regression Analysis | Log-linear regression, R-squared, residual ranking |
| analytics | Star Schema | Dimensional modeling, surrogate keys, composite index |
| analytics | Staged ETL Pipeline | Simulated SQL ETL, staging/production/summary layers |
| analytics | Data Transfer | Cross-system data sync, computed categories, verification |
| analytics | Hierarchical Data | Tree hierarchy, path-based depth, parent flattening |
| analytics | Expression Scoring | Regex expression parsing, complexity scoring, binning |
| analytics | Spatial Data | GeoJSON-like construction, bounding box, geometry filtering |
| analytics | Parallel Export | Multi-endpoint parallel export, fan-in summary |
| analytics | Data Lineage | Provenance tracking, hash-based lineage graph |
| analytics | Pipeline Template | Reusable templates, factory pattern, config overrides |
| analytics | Multi-Pipeline Orchestrator | Independent pipeline orchestration, status rollup |
| analytics | Grand Capstone | End-to-end analytics: all Phase 5 patterns combined |
| dhis2 | DHIS2 Connection | Custom `Block` for DHIS2 credentials, `Secret` for password |
| dhis2 | DHIS2 Block Connection | Named block loading, multi-instance support |
| dhis2 | DHIS2 Org Units | Block-authenticated metadata fetch, nested JSON flattening |
| dhis2 | DHIS2 Data Elements | Data element fetch with categorization, code coverage |
| dhis2 | DHIS2 Indicators | Expression parsing with regex, complexity scoring, binning |
| dhis2 | DHIS2 Geometry | GeoJSON construction, geometry filtering, bounding box |
| dhis2 | DHIS2 Combined Export | Parallel multi-endpoint export with shared block |
| dhis2 | DHIS2 Analytics | Analytics API query, dimension parameters, headers+rows parsing |
| dhis2 | DHIS2 Pipeline | End-to-end DHIS2 pipeline with quality checks and dashboard |
| dhis2 | DHIS2 Env Config | Environment-based configuration strategies |
| dhis2 | DHIS2 Authenticated API | Reusable authenticated API pattern (api_key, bearer, basic) |
| cloud | S3 Parquet Export | Pydantic models, pandas transform, parquet write to S3 (RustFS) |
| cloud | DHIS2 GeoParquet Export | GeoDataFrame, GeoParquet, DHIS2 geometry to S3 |

## Makefile targets

| Target | Description |
|---|---|
| `make help` | Show all available targets |
| `make sync` | Install dependencies (`uv sync`) |
| `make lint` | Run ruff check and mypy |
| `make fmt` | Auto-format with ruff |
| `make test` | Run the test suite |
| `make run` | Run the hello-world flow |
| `make server` | Start a local Prefect server |
| `make start` | Start the full Docker stack |
| `make restart` | Tear down and rebuild the Docker stack |
| `make deploy` | Register blocks, create instances, and deploy all flows |
| `make register-blocks` | Register custom block types (DHIS2) |
| `make create-blocks` | Create DHIS2 credentials block instances for all known servers |
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
