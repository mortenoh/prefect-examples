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

**See:** [Subflows](flow-reference.md#subflows),
[Flow of Flows](flow-reference.md#flow-of-flows),
[Complex Pipeline](flow-reference.md#complex-pipeline)

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

**See:** [Dynamic Tasks](flow-reference.md#dynamic-tasks),
[Complex Pipeline](flow-reference.md#complex-pipeline)

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

**See:** [State Handlers](flow-reference.md#state-handlers),
[Retries and Hooks](flow-reference.md#retries-and-hooks)

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

**See:** [Polling Tasks](flow-reference.md#polling-tasks)

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

**See:** [Concurrency Limits](flow-reference.md#concurrency-limits)

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

**See:** [Variables and Params](flow-reference.md#variables-and-params)

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

**See:** [State Handlers](flow-reference.md#state-handlers),
[Retries and Hooks](flow-reference.md#retries-and-hooks)

## Task caching

Cache task results to avoid redundant computation. Prefect offers three
caching strategies:

```python
from prefect.cache_policies import INPUTS, TASK_SOURCE

# Cache by inputs — same arguments return cached result
@task(cache_policy=INPUTS, cache_expiration=300)
def expensive_computation(x: int, y: int) -> int:
    return x * y

# Cache by source + inputs — invalidate when code changes
@task(cache_policy=TASK_SOURCE + INPUTS)
def source_aware_task(data: str) -> str:
    return data.upper()

# Custom cache key function
def my_cache_key(context, parameters):
    return f"{parameters['category']}:{parameters['item_id']}"

@task(cache_key_fn=my_cache_key, cache_expiration=600)
def cached_lookup(category: str, item_id: int) -> dict: ...
```

Cache hits are only visible during Prefect runtime. Calling `.fn()` always
executes the underlying function.

**See:** [Task Caching](flow-reference.md#task-caching)

## Async patterns

Use `async def` for tasks and flows when working with I/O-bound operations.
Combine with `asyncio.gather()` for concurrent execution:

```python
@task
async def fetch_endpoint(name: str) -> dict:
    await asyncio.sleep(0.5)
    return {"endpoint": name, "records": 100}

@flow
async def concurrent_flow() -> None:
    # Concurrent — total time ≈ max(delays)
    results = await asyncio.gather(
        fetch_endpoint("users"),
        fetch_endpoint("orders"),
        fetch_endpoint("products"),
    )
```

Sync and async tasks can be mixed in an async flow. Sync tasks are called
normally; async tasks are awaited.

**See:** [Async Tasks](flow-reference.md#async-tasks),
[Concurrent Async](flow-reference.md#concurrent-async),
[Async Flow Patterns](flow-reference.md#async-flow-patterns),
[Async Map and Submit](flow-reference.md#async-map-and-submit)

## Artifacts

Publish rich content to the Prefect UI with artifact functions:

```python
from prefect.artifacts import create_markdown_artifact, create_table_artifact, create_link_artifact

# Markdown reports
create_markdown_artifact(key="report", markdown="# Report\n...", description="Daily report")

# Structured tables
create_table_artifact(key="inventory", table=[{"item": "A", "qty": 10}])

# Reference links
create_link_artifact(key="dashboard", link="https://example.com/dashboard")
```

Without a Prefect server, artifact functions silently no-op — tests pass locally.

**See:** [Markdown Artifacts](flow-reference.md#markdown-artifacts),
[Table and Link Artifacts](flow-reference.md#table-and-link-artifacts)

## Blocks and secrets

Blocks provide typed, reusable configuration. The built-in `Secret` block
handles encrypted credentials:

```python
from prefect.blocks.system import Secret

# Load with graceful fallback
try:
    api_key = Secret.load("my-api-key").get()
except ValueError:
    api_key = "dev-fallback"
```

Define custom blocks for typed configuration with `SecretStr` for credentials.
The block stores connection details; `get_client()` returns a dedicated API
client class:

```python
from prefect.blocks.core import Block
from pydantic import Field, SecretStr

class Dhis2Client:
    """Authenticated DHIS2 API client."""

    def __init__(self, base_url: str, username: str, password: str) -> None:
        self._http = httpx.Client(
            base_url=f"{base_url}/api",
            auth=(username, password),
            timeout=60,
        )

    def fetch_metadata(self, endpoint: str) -> list[dict]:
        resp = self._http.get(f"/{endpoint}", params={"paging": "false"})
        resp.raise_for_status()
        return resp.json()[endpoint]

class Dhis2Credentials(Block):
    base_url: str = "https://play.im.dhis2.org/dev"
    username: str = "admin"
    password: SecretStr = Field(default=SecretStr("district"))

    def get_client(self) -> Dhis2Client:
        return Dhis2Client(
            self.base_url,
            self.username,
            self.password.get_secret_value(),
        )
```

The `get_client()` pattern is used by official Prefect integrations (prefect-aws,
prefect-gcp). It returns an authenticated client, keeping credential storage
(Block) separate from API logic (Client).

Blocks can be saved (`block.save("name")`) and loaded (`Block.load("name")`)
from the Prefect server. `SecretStr` fields are encrypted at rest.

### Graceful fallback for development

Load from the server when available, fall back to inline defaults when
developing without a server:

```python
def get_dhis2_credentials() -> Dhis2Credentials:
    try:
        return Dhis2Credentials.load("dhis2")
    except Exception:
        return Dhis2Credentials()  # uses inline defaults
```

### Testing blocks (mock patterns)

Mock at the `Dhis2Client` method level with `@patch.object` to avoid
interfering with Prefect's internal httpx usage:

```python
from unittest.mock import MagicMock, patch

@patch.object(Dhis2Client, "fetch_metadata")
def test_fetch(mock_fetch):
    mock_fetch.return_value = [{"id": "OU1"}, {"id": "OU2"}]
    client = MagicMock(spec=Dhis2Client)
    client.fetch_metadata = mock_fetch
    result = some_task.fn(client)
```

For full flow integration tests, patch `Dhis2Credentials.get_client` to return
a mock `Dhis2Client`:

```python
@patch.object(Dhis2Credentials, "get_client")
def test_flow(mock_get_client):
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = [{"id": "OU1"}]
    mock_get_client.return_value = mock_client
    state = my_flow(return_state=True)
    assert state.is_completed()
```

**See:** [Secret Block](flow-reference.md#secret-block),
[Custom Blocks](flow-reference.md#custom-blocks)

## Notification blocks

Prefect ships with `NotificationBlock` subclasses that provide a unified
`notify(body, subject)` interface for pipeline alerting. The two most common
are `SlackWebhook` and `CustomWebhookNotificationBlock`:

```python
import os
from pydantic import SecretStr
from prefect.blocks.notifications import SlackWebhook, CustomWebhookNotificationBlock

# SlackWebhook -- read URL from environment, never hardcode
slack = SlackWebhook(url=SecretStr(os.environ["SLACK_WEBHOOK_URL"]))
slack.notify(body="Pipeline completed: 150 records processed", subject="Pipeline Alert")

# CustomWebhookNotificationBlock -- flexible for any HTTP endpoint
custom = CustomWebhookNotificationBlock(
    name="ops-webhook",
    url="https://monitoring.example.com/alerts",
    method="POST",
    json_data={"text": "{{subject}}: {{body}}"},
    secrets={"api_token": "my-secret-token"},
)
custom.notify(body="All checks passed", subject="Quality Report")
```

Both blocks call `notify(body, subject)` -- the only difference is how
they build the HTTP request.

### Template resolution

`CustomWebhookNotificationBlock` resolves `{{subject}}`, `{{body}}`,
`{{name}}`, and any key from `secrets` in both the URL and JSON payload:

```python
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
```

When `notify()` is called, every `{{placeholder}}` is replaced with its
value before the HTTP request is sent.

### Flow hooks for production notifications

Attach notification blocks to flow hooks for automatic alerting:

```python
def on_completion_notify(flow, flow_run, state):
    SlackWebhook.load("prod-slack").notify(
        body=f"Flow {flow_run.name!r} completed.",
        subject="Flow Completed",
    )

def on_failure_notify(flow, flow_run, state):
    SlackWebhook.load("prod-slack").notify(
        body=f"CRITICAL: Flow {flow_run.name!r} failed: {state.message}",
        subject="Flow Failed",
    )

@flow(on_completion=[on_completion_notify], on_failure=[on_failure_notify])
def my_pipeline():
    ...
```

Save a block once with `slack.save("prod-slack", overwrite=True)`, then
load it in any hook or task with `SlackWebhook.load("prod-slack")`.

**See:** [Notification Blocks](flow-reference.md#notification-blocks)

## Deployment basics

### Serve vs deploy decision guide

Use `flow.serve()` when:

- You want the simplest possible deployment
- The flow runs on a single machine (dev laptop, cron server, VM)
- No Docker or Kubernetes infrastructure is available

Use `flow.deploy()` when:

- You need infrastructure-level isolation (Docker, K8s)
- Multiple team members trigger runs from the UI
- You want auto-scaling via work pool configuration

Use `prefect.yaml` when:

- You manage multiple deployments declaratively
- You want deployment config in version control
- You follow a GitOps workflow

### flow.serve()

The simplest deployment -- runs in-process with optional scheduling:

```python
my_flow.serve(name="my-deployment", cron="0 6 * * *")
```

`flow.serve()` blocks the process and polls for scheduled runs. It is the
Prefect equivalent of placing a DAG file in Airflow's `dags/` folder.

### flow.deploy()

Production deployment -- sends runs to a work pool:

```python
my_flow.deploy(
    name="my-deployment",
    work_pool_name="my-pool",
    cron="0 6 * * *",
)
```

Work pools define where work runs (process, docker, kubernetes). Workers poll
pools for scheduled runs.

### Parameterized deployments

Pass default parameters at deployment time. These can be overridden per run
from the UI, API, or CLI:

```python
# In code
my_flow.serve(
    name="dhis2-sync",
    cron="0 6 * * *",
    parameters={"endpoints": ["organisationUnits", "dataElements"]},
)

# In prefect.yaml
# deployments:
#   - name: dhis2-ou
#     entrypoint: deployments/dhis2_ou/flow.py:dhis2_ou_flow
```

Override at run time:

```bash
prefect deployment run dhis2_ou/dhis2-ou
```

### `prefect.yaml` pattern

Define multiple deployments in a single YAML file at the project root:

```yaml
deployments:
  - name: dhis2-ou
    entrypoint: flow.py:dhis2_ou_flow
    schedules:
      - cron: "0 6 * * *"
        timezone: "UTC"
    work_pool:
      name: default

  - name: etl-every-5m
    entrypoint: flows/core/core_flow_serve.py:flow_serve_flow
    schedules:
      - cron: "*/5 * * * *"
    work_pool:
      name: default
```

Deploy all at once with `prefect deploy --all` or individually with
`prefect deploy -n dhis2-ou`.

### Deployment-aware flows with `prefect.runtime`

Use `prefect.runtime` to access deployment context inside a running flow:

```python
from prefect.runtime import deployment, flow_run

deployment_name = deployment.name            # None for local runs
flow_run_name = flow_run.name                # auto-generated name
scheduled_start = flow_run.scheduled_start_time
```

This lets a single flow produce different output depending on whether it is
running locally or as a scheduled deployment.

### Work pool setup walkthrough

```bash
# 1. Create a process-based work pool
prefect work-pool create my-pool --type process

# 2. Start a worker that polls the pool
prefect worker start --pool my-pool

# 3. Deploy a flow to the pool
prefect deploy -n dhis2-ou

# 4. Trigger a run manually
prefect deployment run dhis2_ou/dhis2-ou
```

### Managing deployments with the CLI

```bash
# List and inspect
prefect deployment ls
prefect deployment inspect <flow/deployment>

# Schedule management
prefect deployment set-schedule <name> --cron "0 8 * * *"
prefect deployment set-schedule <name> --interval 3600
prefect deployment set-schedule <name> --rrule "FREQ=WEEKLY;BYDAY=MO,WE,FR"
prefect deployment clear-schedule <name>

# Pause and resume
prefect deployment pause <name>
prefect deployment resume <name>

# Trigger runs with parameter overrides
prefect deployment run <name> -p key=value

# Cleanup
prefect deployment delete <name>
```

### Dev/prod deployment pattern

Use `flow.serve()` during development for fast iteration, then switch to
`flow.deploy()` (or `prefect.yaml`) for production:

```python
if __name__ == "__main__":
    # Dev: run directly
    # dhis2_ou_flow()

    # Dev: serve with schedule
    # dhis2_ou_flow.serve(name="dev-sync", cron="*/5 * * * *")

    # Prod: deploy to work pool (or use prefect.yaml)
    # dhis2_ou_flow.deploy(
    #     name="dhis2-ou",
    #     work_pool_name="my-pool",
    #     cron="0 6 * * *",
    # )
    dhis2_ou_flow()
```

### Deploying DHIS2 flows

The `deployments/` directory contains production deployment examples, each
with its own `flow.py`, `prefect.yaml`, and `deploy.py`:

- **`dhis2_connection/`** -- connection check and server status artifact
- **`dhis2_ou/`** -- org unit listing with markdown artifact

**See:** [Flow Serve](flow-reference.md#flow-serve),
[Schedules](flow-reference.md#schedules),
[Work Pools](flow-reference.md#work-pools),
[Deployments directory](flow-reference.md#deployments-directory)

## Pydantic models for type-safe pipelines

Use Pydantic `BaseModel` as task parameters and return types for automatic
validation and serialisation:

```python
from pydantic import BaseModel

class UserRecord(BaseModel):
    name: str
    email: str
    age: int

class ProcessingResult(BaseModel):
    records: list[dict]
    errors: list[str]
    summary: str

@task
def validate_users(users: list[UserRecord]) -> ProcessingResult:
    valid, errors = [], []
    for user in users:
        if user.age < 0:
            errors.append(f"Invalid age for {user.name}")
        else:
            valid.append(user.model_dump())
    return ProcessingResult(records=valid, errors=errors, summary=f"{len(valid)} valid")
```

Pydantic replaces XCom serialisation pain with automatic validation, type safety,
and clean `.model_dump()` for dict conversion.

**See:** [Pydantic Models](flow-reference.md#pydantic-models),
[Pydantic Validation](flow-reference.md#pydantic-validation)

## Shell and HTTP tasks

Replace Airflow's BashOperator and HttpOperator with plain Python in `@task`
functions:

```python
# Shell: subprocess.run() replaces BashOperator
@task
def run_command(cmd: str) -> str:
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
    return result.stdout.strip()

# HTTP: httpx replaces HttpOperator
@task
def http_get(url: str) -> dict:
    response = httpx.get(url, timeout=10.0)
    response.raise_for_status()
    return response.json()
```

No special operators needed. Standard Python libraries inside tasks are the
Prefect way.

**See:** [Shell Tasks](flow-reference.md#shell-tasks),
[HTTP Tasks](flow-reference.md#http-tasks)

## Error handling with quarantine pattern

Separate good and bad records during processing, capturing error reasons:

```python
from pydantic import BaseModel

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

The quarantine pattern prevents a few bad records from failing the entire pipeline.

**See:** [Error Handling ETL](flow-reference.md#error-handling-etl)

## Transactions for atomic operations

Group tasks atomically with `transaction()` -- if any task fails, the group
is treated as a unit:

```python
from prefect.transactions import transaction

@flow
def atomic_pipeline():
    with transaction():
        step_a()
        step_b()
        step_c()
```

Transactions are a Prefect-specific feature with no direct Airflow equivalent.

**See:** [Transactions](flow-reference.md#transactions)

## Task runners for concurrent execution

Choose the right task runner for your workload:

```python
from prefect.task_runners import ThreadPoolTaskRunner

# I/O-bound: ThreadPoolTaskRunner for concurrent execution
@flow(task_runner=ThreadPoolTaskRunner(max_workers=3))
def io_flow():
    futures = fetch_data.map(urls)
    return [f.result() for f in futures]

# CPU-bound: default runner (or ConcurrentTaskRunner)
@flow
def cpu_flow():
    futures = compute.map(inputs)
    return [f.result() for f in futures]
```

`ThreadPoolTaskRunner` provides concurrent execution for I/O-bound tasks like
API calls and file reads.

**See:** [Task Runners](flow-reference.md#task-runners)

## File I/O patterns

Use stdlib `csv` and `json` for file-based pipelines. `tempfile.mkdtemp()`
provides isolated working directories, and `pathlib.Path` keeps paths clean:

```python
@task
def read_csv(path: Path) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))

@task
def write_csv(directory: str, filename: str, rows: list[dict]) -> Path:
    path = Path(directory) / filename
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return path
```

For mixed file types, dispatch on suffix:

```python
@task
def read_file(path: Path) -> list[dict]:
    if path.suffix == ".csv":
        return list(csv.DictReader(open(path, newline="")))
    elif path.suffix == ".json":
        return json.loads(path.read_text())
```

**See:** [CSV File Processing](flow-reference.md#csv-file-processing),
[JSON Event Ingestion](flow-reference.md#json-event-ingestion),
[Multi-File Batch Processing](flow-reference.md#multi-file-batch-processing)

## Data quality rules engine

Define quality rules as configuration and dispatch them to check functions:

```python
class QualityRule(BaseModel):
    name: str
    rule_type: str
    column: str = ""
    params: dict = {}

@task
def execute_rule(data: list[dict], rule: QualityRule) -> RuleResult:
    if rule.rule_type == "not_null":
        return run_not_null_check.fn(data, rule.column)
    elif rule.rule_type == "range":
        return run_range_check.fn(data, rule.column, ...)
```

Compute an overall score and classify as green (>= 0.9), amber (>= 0.7),
or red (< 0.7).

**See:** [Quality Rules Engine](flow-reference.md#quality-rules-engine),
[Cross-Dataset Validation](flow-reference.md#cross-dataset-validation)

## Configuration-driven pipelines

Control pipeline behaviour entirely through configuration dicts:

```python
class PipelineConfig(BaseModel):
    name: str
    stages: list[StageConfig]
    fail_fast: bool = True

@flow
def pipeline(raw_config: dict):
    config = parse_config(raw_config)
    for stage in config.stages:
        if stage.enabled:
            dispatch_stage(stage, context)
```

Different configs produce different pipeline runs through the same flow code.
Disabled stages are skipped automatically.

**See:** [Config-Driven Pipeline](flow-reference.md#config-driven-pipeline)

## Producer-consumer pattern

Decouple data production from consumption using file-based data contracts:

```python
@flow
def producer_consumer_flow():
    producer_flow(data_dir, producer_id="alpha")
    producer_flow(data_dir, producer_id="beta")
    results = consumer_flow(data_dir, consumer_id="main")
```

Producers write JSON data + metadata files. Consumers discover packages by
scanning for metadata files. Each flow is independently testable.

**See:** [Producer-Consumer](flow-reference.md#producer-consumer)

## Circuit breaker pattern

Prevent cascading failures by opening a circuit after consecutive failures:

```python
@task
def call_with_circuit(circuit: CircuitState, should_succeed: bool):
    if circuit.state == "open":
        circuit = circuit.model_copy(update={"state": "half_open"})
    # Execute call, track failures, trip if threshold reached
```

States: closed (normal) -> open (fail-fast) -> half_open (probe) -> closed
(recovery). Deterministic boolean outcomes make testing straightforward.

**See:** [Circuit Breaker](flow-reference.md#circuit-breaker)

## Incremental processing with manifests

Track processed files in a JSON manifest to avoid reprocessing:

```python
@task
def identify_new_files(all_files: list[Path], manifest: ProcessingManifest) -> list[Path]:
    return [f for f in all_files if f.name not in manifest.processed_files]
```

Run the flow twice: the second run processes zero files. This is the foundation
for idempotent file pipelines.

**See:** [Incremental Processing](flow-reference.md#incremental-processing),
[Idempotent Operations](flow-reference.md#idempotent-operations)

## Checkpoint-based recovery

Save progress after each stage. On re-run, skip completed stages:

```python
for stage in stages:
    if not should_run_stage.fn(store, stage):
        recovered += 1
        continue
    result = execute_stage.fn(stage, context)
    save_checkpoint.fn(store, stage, "completed", result, path)
```

Fail at stage X, fix the issue, re-run, and stages before X are automatically
skipped.

**See:** [Error Recovery](flow-reference.md#error-recovery)

## Application-level API caching

Cache API responses in a dict with hashlib-based keys and TTL expiry:

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

Track hit/miss rates to measure cache effectiveness.

**See:** [Response Caching](flow-reference.md#response-caching)

## Threshold classification and advisories (flow 081)

Classify values against an ordered list of thresholds and generate advisories:

```python
AQI_THRESHOLDS: list[tuple[float, str, str]] = [
    (50.0, "Good", "green"),
    (100.0, "Moderate", "yellow"),
    (150.0, "Unhealthy for Sensitive Groups", "orange"),
    (200.0, "Unhealthy", "red"),
    (300.0, "Very Unhealthy", "purple"),
    (float("inf"), "Hazardous", "maroon"),
]

for threshold, cat, col in AQI_THRESHOLDS:
    if mean_val <= threshold:
        category = cat
        color = col
        break
```

Walk the thresholds in order and stop at the first match. Use a separate
severity ordering list to rank worst outcomes.

**See:** [Air Quality Index](flow-reference.md#air-quality-index)

## Composite risk scoring (flow 082)

Normalize risk factors to a common scale, then compute a weighted composite:

```python
marine_avg = sum(f.normalized_score for f in marine_factors) / len(marine_factors)
flood_avg = sum(f.normalized_score for f in flood_factors) / len(flood_factors)
weighted = marine_avg * marine_weight + flood_avg * flood_weight
```

Configurable weights allow tuning the relative importance of each risk source.

**See:** [Composite Risk Assessment](flow-reference.md#composite-risk-assessment)

## Pearson correlation (flows 086, 088)

Manual Pearson correlation using only `math` and `statistics`:

```python
def _pearson(x: list[float], y: list[float]) -> float:
    mean_x = statistics.mean(x)
    mean_y = statistics.mean(y)
    num = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y, strict=True))
    den_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
    den_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))
    return num / (den_x * den_y)
```

This pattern appears in flows 083, 086, 087, and 088. No numpy or scipy
required.

**See:** [Multi-Indicator Correlation](flow-reference.md#multi-indicator-correlation),
[Hypothesis Testing](flow-reference.md#hypothesis-testing)

## Log-linear regression (flow 089)

Manual OLS regression with R-squared and residual-based ranking:

```python
cov_xy = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y, strict=True)) / n
var_x = sum((xi - mean_x) ** 2 for xi in x) / n
slope = cov_xy / var_x
intercept = mean_y - slope * mean_x
ss_res = sum((yi - (slope * xi + intercept)) ** 2 for xi, yi in zip(x, y, strict=True))
ss_tot = sum((yi - mean_y) ** 2 for yi in y)
r_squared = 1.0 - (ss_res / ss_tot)
```

Log-transform skewed data before regression. Rank entities by residual:
negative residual means better-than-predicted performance.

**See:** [Regression Analysis](flow-reference.md#regression-analysis)

## Dimensional modeling (Star schema) (flow 090)

Build fact and dimension tables with surrogate keys and composite index:

```python
@task
def build_country_dimension(data: list[dict]) -> list[DimCountry]:
    for i, d in enumerate(data, 1):
        dims.append(DimCountry(key=i, name=d["name"], region=d["region"], ...))

@task
def min_max_normalize(values: list[float], higher_is_better: bool) -> list[float]:
    normalized = [(v - min_v) / (max_v - min_v) for v in values]
    if not higher_is_better:
        normalized = [1.0 - n for n in normalized]
    return normalized
```

Surrogate keys are assigned sequentially. Min-max normalization respects the
`higher_is_better` flag. Weighted normalized indicators produce a composite
ranking.

**See:** [Star Schema](flow-reference.md#star-schema)

## Simulated SQL ETL (flow 091)

Three-layer ETL: staging, production, summary:

```python
raw = generate_raw_data()
staging = load_staging(raw)                   # raw + timestamp
production = validate_and_transform(staging)  # parsed + is_valid flag
valid = filter_valid(production)
summary = compute_summary(valid, "category")  # grouped stats
```

Each layer adds metadata. Invalid records carry `is_valid=False` through the
production layer without being dropped.

**See:** [Staged ETL Pipeline](flow-reference.md#staged-etl-pipeline)

## Regex expression parsing (flow 094)

Count operands and operators to score expression complexity:

```python
OPERAND_PATTERN = re.compile(r"#\{[^}]+\}")

num_operands = len(OPERAND_PATTERN.findall(expression))
num_operators = sum(1 for c in expression if c in "+-*/")
total = num_operands + num_operators
```

Bin scores into trivial/simple/moderate/complex categories for reporting.

**See:** [Expression Complexity Scoring](flow-reference.md#expression-complexity-scoring)

## Data lineage tracking (flow 097)

Hash-based provenance through pipeline stages:

```python
@task
def compute_data_hash(records: list[DataRecord]) -> str:
    raw = str(sorted(str(r.model_dump()) for r in records))
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
```

Each transformation records input_hash and output_hash. The lineage graph shows
which stages modified data (input_hash != output_hash) and which were passthrough.

**See:** [Data Lineage Tracking](flow-reference.md#data-lineage-tracking)

## Pipeline templates (flow 098)

Factory pattern for reusable pipeline configurations:

```python
etl_basic = create_template("etl_basic", [
    StageTemplate(name="extract", stage_type="extract", default_params={"batch_size": 100}),
    StageTemplate(name="load", stage_type="load", default_params={"batch_size": 100}),
])

basic_small = instantiate_template(etl_basic, {"batch_size": 50})
basic_large = instantiate_template(etl_basic, {"batch_size": 500})
```

Define a template once, instantiate with different overrides. Stage execution
merges overrides into default parameters.

**See:** [Pipeline Template Factory](flow-reference.md#pipeline-template-factory)

## Custom blocks for API integration

Subclass `Block` to create typed connection objects for external APIs. This is
the Prefect equivalent of Airflow's `BaseHook.get_connection()`. The credentials
block stores connection details; `get_client()` returns a dedicated API client:

```python
from prefect.blocks.core import Block
from pydantic import Field, SecretStr

class Dhis2Client:
    """Authenticated DHIS2 API client."""

    def __init__(self, base_url: str, username: str, password: str) -> None:
        self._http = httpx.Client(
            base_url=f"{base_url}/api",
            auth=(username, password),
            timeout=60,
        )

    def get_server_info(self) -> dict:
        resp = self._http.get("/system/info")
        resp.raise_for_status()
        return resp.json()

    def fetch_metadata(self, endpoint: str, fields: str = ":owner") -> list[dict]:
        resp = self._http.get(f"/{endpoint}", params={"paging": "false", "fields": fields})
        resp.raise_for_status()
        return resp.json()[endpoint]

class Dhis2Credentials(Block):
    _block_type_name = "dhis2-credentials"
    base_url: str = Field(default="https://play.im.dhis2.org/dev")
    username: str = Field(default="admin")
    password: SecretStr = Field(default=SecretStr("district"))

    def get_client(self) -> Dhis2Client:
        return Dhis2Client(
            self.base_url,
            self.username,
            self.password.get_secret_value(),
        )

# Load from server with fallback to inline defaults
def get_dhis2_credentials() -> Dhis2Credentials:
    try:
        return Dhis2Credentials.load("dhis2")
    except Exception:
        return Dhis2Credentials()
```

Usage in flows is clean -- the block provides credentials, the client provides
API methods:

```python
creds = get_dhis2_credentials()
client = creds.get_client()
info = client.get_server_info()            # authenticated API call
units = client.fetch_metadata("organisationUnits")  # returns list[dict]
```

Register a block once via the Prefect UI or `Dhis2Credentials(...).save("dhis2")`.
All flows that need the connection call `Dhis2Credentials.load("dhis2")`.

For generic authenticated APIs, use a pluggable auth block:

```python
class ApiAuthConfig(Block):
    auth_type: str  # "api_key", "bearer", "basic"
    base_url: str

def build_auth_header(config: ApiAuthConfig, credentials: str) -> AuthHeader:
    if config.auth_type == "api_key":
        return AuthHeader(header_name="X-API-Key", header_value=credentials)
    elif config.auth_type == "bearer":
        return AuthHeader(header_name="Authorization",
                          header_value=f"Bearer {credentials}")
    ...
```

**See:** [DHIS2 Connection Block](flow-reference.md#dhis2-connection-block),
[Authenticated API Pipeline](flow-reference.md#authenticated-api-pipeline)

## Secret management strategies

Prefect provides multiple ways to manage secrets. Choose based on your
environment:

```python
# 1. SecretStr on a Block (recommended -- keeps credentials with config)
from pydantic import SecretStr
class MyConnection(Block):
    password: SecretStr = Field(default=SecretStr("dev-password"))

# 2. Secret block (standalone credential)
from prefect.blocks.system import Secret
password = Secret.load("dhis2-password").get()

# 3. Environment variable (simple, works everywhere)
import os
password = os.environ.get("DHIS2_PASSWORD", "fallback")

# 4. JSON block (for structured config)
from prefect.blocks.system import JSON
config = JSON.load("dhis2-config").value
```

The preferred pattern is `SecretStr` directly on the connection block. This
keeps credentials co-located with the connection config they belong to:

```python
creds = Dhis2Credentials.load("dhis2")  # password included, encrypted at rest
client = creds.get_client()
client.fetch_metadata("organisationUnits")  # no separate password argument
```

Always use graceful fallbacks so flows work in development (no server) and
production (with server):

```python
def get_dhis2_credentials() -> Dhis2Credentials:
    try:
        return Dhis2Credentials.load("dhis2")
    except Exception:
        return Dhis2Credentials()  # uses inline defaults
```

**See:** [Environment-Based Configuration](flow-reference.md#environment-based-configuration),
[DHIS2 Connection Block](flow-reference.md#dhis2-connection-block)
