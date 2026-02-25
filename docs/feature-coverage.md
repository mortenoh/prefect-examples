# Feature Coverage

Prefect 3 features covered by this example repository, and gaps still to fill.

## Coverage matrix

| Feature | Covered | Example flows | Notes |
|---------|---------|---------------|-------|
| `@flow` / `@task` | Yes | Basics 1--20 | Core building blocks |
| Subflows | Yes | Subflows, Flow of Flows, Complex Pipeline | Nested `@flow` calls |
| `.map()` / `.submit()` | Yes | Dynamic Tasks, Advanced Map Patterns | Fan-out and parallel execution |
| Retries / retry delays | Yes | Retries and Hooks, Advanced Retries | Lists, jitter, condition functions |
| Task caching | Yes | Task Caching | `INPUTS`, `TASK_SOURCE`, `cache_key_fn` |
| Timeouts | Yes | Task Timeouts | `timeout_seconds` on tasks and flows |
| State hooks | Yes | State Handlers, Failure Escalation | `on_failure`, `on_completion` |
| `allow_failure` | Yes | Advanced State Handling | Downstream continues after failure |
| Parameters | Yes | Parameterized Flows | Typed defaults, runtime overrides |
| Results / persistence | Yes | Task Results, Result Persistence | `persist_result`, `result_storage_key` |
| Artifacts | Yes | Markdown Artifacts, Table and Link Artifacts | Markdown, tables, links in UI |
| Tags | Yes | Tags | Decorator and context-manager |
| Custom run names | Yes | Task Run Names, Flow Run Names | Templates and callables |
| Structured logging | Yes | Structured Logging | `get_run_logger()`, `log_prints` |
| Events | Yes | Events | `emit_event()` |
| Blocks / Secret | Yes | Secret Block, Custom Blocks, DHIS2 flows | `Block` subclass, `SecretStr` |
| Variables | Yes | Variables and Params | `Variable.get()` / `set()` |
| Concurrency limits (local) | Yes | Concurrency Limits | `concurrency()` context manager |
| Async tasks/flows | Yes | Async Tasks, Concurrent Async, Async Flow Patterns | `asyncio.gather()` |
| `flow.serve()` | Yes | Flow Serve | In-process scheduling |
| `flow.deploy()` | Yes | Work Pools | Work pool + worker |
| `prefect.yaml` | Yes | Deployments directory | Declarative deployments |
| Schedules (cron/interval/rrule) | Yes | Schedules | All three schedule types |
| `prefect.runtime` | Yes | Runtime Context | Deployment-aware flows |
| Transactions | Yes | Transactions | `transaction()` for atomic groups |
| Interactive flows | Yes | Interactive Flows | `pause_flow_run()` |
| Task runners (ThreadPool) | Yes | Task Runners | `ThreadPoolTaskRunner` |
| Pydantic models | Yes | Pydantic Models, Pydantic Validation | Type-safe data passing |
| S3 storage (prefect-aws) | Yes | S3 Parquet Export, DHIS2 GeoParquet Export | `S3Bucket`, `MinIOCredentials` |
| Basic auth | Yes | Docker Compose stack | `PREFECT_SERVER_API_AUTH_STRING` / `PREFECT_API_AUTH_STRING` |

## Gaps -- features not yet covered

### High priority (core Prefect 3 differentiators)

| Feature | What it does | Complexity | Why it matters |
|---------|-------------|------------|----------------|
| **Automations and triggers** | Event-driven flow execution, deployment triggers, state-change reactions | Advanced | Core Prefect 3 differentiator for reactive orchestration |
| **Global concurrency limits** | Cross-flow resource throttling via API (vs local `concurrency()`) | Intermediate | Important for shared-resource coordination across deployments |
| **Webhooks** | Receive external HTTP events, transform to Prefect events via Jinja2 templates | Intermediate | Enables integration with external systems (GitHub, Slack, CI) |
| **Remote result storage** | Store task results in S3/GCS/Azure instead of local filesystem | Intermediate | Required for production distributed execution |

### Medium priority (commonly used integrations)

| Feature | What it does | Complexity | Why it matters |
|---------|-------------|------------|----------------|
| **prefect-sqlalchemy** | Database connections, parameterized SQL queries | Intermediate | Most common integration for data engineering |
| **prefect-dbt** | dbt Cloud/Core orchestration from Prefect flows | Advanced | Popular for analytics engineering teams |
| **Notification blocks** | Slack/Discord/Teams/PagerDuty via built-in blocks (not raw httpx) | Intermediate | Production alerting without manual HTTP calls |
| **Docker/K8s work pools** | Push-based execution to containers | Advanced | Production infrastructure patterns |
| **GitHub storage deployments** | Pull flow code from GitHub repos at runtime | Intermediate | GitOps deployment workflow |
| **DaskTaskRunner / RayTaskRunner** | Distributed task execution for CPU-heavy workloads | Advanced | Scaling beyond single-machine parallelism |

### Lower priority (nice to have)

| Feature | What it does | Complexity |
|---------|-------------|------------|
| **Managed execution** | Prefect Cloud managed infrastructure | Intermediate |
| **Custom event triggers** | Complex event-driven patterns with compound triggers | Advanced |
| **Work pool priority** | Priority-based flow run scheduling across pools | Intermediate |
| **Flow run infrastructure overrides** | Per-run infrastructure customization | Intermediate |
| **Reverse proxy auth (SSO/OIDC)** | oauth2-proxy + nginx/Caddy/Traefik for multi-user auth | Advanced |
| **RBAC** | Role-based access control (Prefect Cloud only) | N/A |
