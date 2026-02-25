# Tutorials

Step-by-step walkthroughs for common Prefect tasks using this project.

---

## Tutorial 1: Your first flow

Write a flow with tasks, run it locally, and view the results.

### Step 1 -- Create the flow

Create a file `flows/my_first_flow.py`:

```python
from prefect import flow, task


@task
def extract() -> list[dict]:
    """Simulate data extraction."""
    return [
        {"name": "Alice", "score": 85},
        {"name": "Bob", "score": 92},
    ]


@task
def transform(records: list[dict]) -> list[dict]:
    """Add a pass/fail field."""
    for r in records:
        r["status"] = "pass" if r["score"] >= 70 else "fail"
    return records


@task
def load(records: list[dict]) -> None:
    """Print the results."""
    for r in records:
        print(f"{r['name']}: {r['score']} ({r['status']})")


@flow(name="my_first_flow", log_prints=True)
def my_first_flow() -> None:
    raw = extract()
    processed = transform(raw)
    load(processed)


if __name__ == "__main__":
    my_first_flow()
```

### Step 2 -- Run it

```bash
uv run python flows/my_first_flow.py
```

You should see Prefect log output showing the flow and each task completing,
followed by the printed results.

### Step 3 -- View in the Prefect UI

Start a Prefect server and run the flow again:

```bash
# Terminal 1: start the server
uv run prefect server start

# Terminal 2: run the flow
uv run python flows/my_first_flow.py
```

Open `http://localhost:4200` to see the flow run, task runs, and their states.

### What you learned

- `@task` turns a function into a tracked unit of work
- `@flow` is the top-level orchestration container
- `log_prints=True` captures `print()` output in Prefect logs
- Return values flow between tasks naturally -- no XCom needed

**Related flows:** [Hello World](flow-reference.md#hello-world),
[Python Tasks](flow-reference.md#python-tasks),
[Taskflow ETL](flow-reference.md#taskflow-etl)

---

## Tutorial 2: Working with blocks

Create a `Dhis2Credentials` block, save it, and use it in a flow.

### Step 1 -- Create and save a block

```python
from prefect_examples.dhis2 import Dhis2Credentials

creds = Dhis2Credentials(
    base_url="https://play.im.dhis2.org/dev",
    username="admin",
    # password defaults to "district"
)

# Save to the Prefect server (requires a running server)
creds.save("dhis2", overwrite=True)
print("Block saved!")
```

Run this once to register the block:

```bash
uv run python -c "
from prefect_examples.dhis2 import Dhis2Credentials
Dhis2Credentials().save('dhis2', overwrite=True)
print('Block saved!')
"
```

### Step 2 -- Load and use the block

```python
from prefect import flow, task
from prefect_examples.dhis2 import Dhis2Credentials, Dhis2Client


@task
def fetch_org_units(client: Dhis2Client) -> list[dict]:
    return client.fetch_metadata("organisationUnits", fields="id,name,level")


@flow(name="block_demo", log_prints=True)
def block_demo() -> None:
    creds = Dhis2Credentials.load("dhis2")
    client = creds.get_client()
    units = fetch_org_units(client)
    print(f"Fetched {len(units)} org units")


if __name__ == "__main__":
    block_demo()
```

### Step 3 -- Graceful fallback

Use `get_dhis2_credentials()` so the flow works with or without a server:

```python
from prefect_examples.dhis2 import get_dhis2_credentials

creds = get_dhis2_credentials()  # loads from server or uses inline defaults
client = creds.get_client()
```

### What you learned

- Blocks are typed configuration objects that can be saved/loaded from the server
- `SecretStr` fields are encrypted at rest when saved
- `get_client()` returns an authenticated API client
- The fallback pattern keeps flows runnable without a server

**Related flows:** [DHIS2 Connection Block](flow-reference.md#dhis2-connection-block),
[DHIS2 Org Units API](flow-reference.md#dhis2-org-units-api)

---

## Tutorial 3: Variables and configuration

Use `Variable.set()`/`Variable.get()` for runtime configuration.

### Step 1 -- Set a variable

```python
from prefect.variables import Variable

Variable.set("batch_config", '{"batch_size": 100, "retries": 3}', overwrite=True)
```

### Step 2 -- Use it in a flow

```python
import json
from prefect import flow, task
from prefect.variables import Variable


@task
def get_config() -> dict:
    raw = Variable.get("batch_config", default='{"batch_size": 50}')
    return json.loads(raw)


@task
def process_batch(config: dict) -> None:
    print(f"Processing with batch_size={config['batch_size']}")


@flow(name="variable_demo", log_prints=True)
def variable_demo() -> None:
    config = get_config()
    process_batch(config)


if __name__ == "__main__":
    variable_demo()
```

### When to use what

| Mechanism | Best for | Example |
|---|---|---|
| `Variable` | Simple key-value runtime config | Batch sizes, feature flags |
| Custom `Block` | Typed connection config with methods | `Dhis2Credentials` |
| `Secret` block | Single credential values | API keys, tokens |
| `JSON` block | Structured configuration | Threshold mappings |
| Environment variables | CI/CD, container config | `DHIS2_PASSWORD` |
| Flow parameters | Per-run overrides | `--param batch_size=200` |

### What you learned

- Variables are simple string key-value pairs stored on the Prefect server
- `Variable.get()` accepts a default for offline development
- Variables complement blocks and parameters -- they are the simplest config
  mechanism

**Related flows:** [Variables and Params](flow-reference.md#variables-and-params)

---

## Tutorial 4: Deploying a flow

Create a deployment, register it, trigger a run, and manage schedules.

### Step 1 -- Create the deployment directory

```bash
mkdir -p deployments/my_flow
```

### Step 2 -- Write the flow

Create `deployments/my_flow/flow.py`:

```python
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact


@task
def do_work() -> str:
    return "Hello from a deployment!"


@flow(name="my_deployed_flow", log_prints=True)
def my_deployed_flow() -> None:
    result = do_work()
    print(result)
    create_markdown_artifact(
        key="deployment-result",
        markdown=f"## Result\n\n{result}",
    )


if __name__ == "__main__":
    my_deployed_flow()
```

### Step 3 -- Write `prefect.yaml`

Create `deployments/my_flow/prefect.yaml`:

```yaml
pull:
  - prefect.deployments.steps.set_working_directory:
      directory: /opt/prefect/deployments/my_flow

deployments:
  - name: my-flow
    entrypoint: flow.py:my_deployed_flow
    schedules:
      - cron: "0 6 * * *"
        timezone: "UTC"
    work_pool:
      name: default
```

The `pull` step tells the worker where to find the flow source inside the
Docker container.

### Step 4 -- Deploy

```bash
cd deployments/my_flow
PREFECT_API_URL=http://localhost:4200/api uv run prefect deploy --all
```

Or add it to the `Makefile`'s `deploy` target.

### Step 5 -- Trigger a run

```bash
PREFECT_API_URL=http://localhost:4200/api \
  uv run prefect deployment run my_deployed_flow/my-flow
```

### Step 6 -- Manage the schedule

```bash
# Change to every 30 minutes
prefect deployment set-schedule my_deployed_flow/my-flow --interval 1800

# Pause scheduling
prefect deployment pause my_deployed_flow/my-flow

# Resume scheduling
prefect deployment resume my_deployed_flow/my-flow

# Remove all schedules
prefect deployment clear-schedule my_deployed_flow/my-flow
```

### Schedule types

You can use three types of schedules when deploying:

**Cron** -- standard cron expressions:

```yaml
schedules:
  - cron: "*/15 * * * *"    # every 15 minutes
    timezone: "UTC"
```

**Interval** -- fixed number of seconds:

```yaml
schedules:
  - interval: 900           # every 15 minutes
```

**RRule** -- RFC 5545 recurrence rules:

```yaml
schedules:
  - rrule: "FREQ=WEEKLY;BYDAY=MO,WE,FR"
    timezone: "UTC"
```

Multiple schedules can be combined on a single deployment:

```yaml
schedules:
  - cron: "0 6 * * *"
    timezone: "UTC"
  - cron: "0 18 * * *"
    timezone: "UTC"
```

### What you learned

- Deployments package a flow for scheduled or on-demand execution
- `prefect.yaml` defines the entrypoint, schedule, and work pool
- Schedules support cron, interval, and RRule formats
- Schedules can be updated after deployment via CLI or UI
- The `pull` step configures where the worker finds the flow source

**Related flows:** [Flow Serve](flow-reference.md#flow-serve),
[Schedules](flow-reference.md#schedules)

---

## Tutorial 5: Running the Docker stack

Start the full Prefect environment, deploy flows, and monitor runs.

### Step 1 -- Start the stack

```bash
make start
```

This runs `docker compose up --build` and starts four services:

| Service | Port | Purpose |
|---|---|---|
| PostgreSQL | 5432 | Database backend |
| Prefect Server | 4200 | UI + API |
| Prefect Worker | -- | Executes flow runs |
| RustFS | 9000, 9001 | S3-compatible object storage |

Wait for all services to be healthy (watch the healthcheck logs).

### Step 2 -- Verify services

Open `http://localhost:4200` in your browser. You should see the Prefect UI
with an empty dashboard.

Check the worker is connected:

```bash
docker compose logs prefect-worker | tail -5
```

You should see the worker polling the `default` work pool.

### Step 3 -- Deploy flows

In a separate terminal:

```bash
make deploy
```

This registers the `dhis2-connection` and `dhis2-ou` deployments. Refresh the
UI and navigate to the Deployments page to see them.

### Step 4 -- Trigger a run

```bash
PREFECT_API_URL=http://localhost:4200/api \
  uv run prefect deployment run dhis2_connection/dhis2-connection
```

Or click the "Run" button in the UI.

### Step 5 -- View results

Navigate to the Flow Runs page in the UI. Click on the run to see:

- **Timeline** -- task execution order and duration
- **Logs** -- captured print output and Prefect log messages
- **Artifacts** -- markdown reports and tables created by the flow

### Step 6 -- Manage schedules

Both deployments are configured to run every 15 minutes. To change the
schedule:

```bash
# Switch to hourly
PREFECT_API_URL=http://localhost:4200/api \
  uv run prefect deployment set-schedule dhis2_ou/dhis2-ou --interval 3600

# Pause the schedule
PREFECT_API_URL=http://localhost:4200/api \
  uv run prefect deployment pause dhis2_ou/dhis2-ou
```

### Step 7 -- Shut down

Press `Ctrl+C` in the terminal running `make start`, or:

```bash
docker compose down       # stop services, keep data
docker compose down -v    # stop services and delete all data
```

### What you learned

- `make start` brings up the complete Prefect environment
- `make deploy` registers deployments with the server
- The worker automatically picks up scheduled and manually triggered runs
- Deployment schedules can be viewed and managed from the UI or CLI
- `docker compose down -v` provides a clean reset

**Related pages:** [Infrastructure](infrastructure.md),
[CLI Reference](cli-reference.md)

---

## Tutorial 6: Testing with Slack webhooks

Send real Slack notifications from a Prefect flow using notification blocks.

### Step 1 -- Create a Slack app

1. Go to [api.slack.com/apps](https://api.slack.com/apps) and click
   **Create New App** > **From scratch**.
2. Name it (e.g. "Prefect Alerts") and pick a workspace.
3. In the left sidebar, click **Incoming Webhooks** and toggle it **On**.
4. Click **Add New Webhook to Workspace**, choose a channel, and click
   **Allow**.
5. Copy the webhook URL -- it looks like
   `https://hooks.slack.com/services/T00/B00/xxxx`.

### Step 2 -- Test with curl

Verify the URL works before writing any Python:

```bash
curl -X POST -H 'Content-Type: application/json' \
  -d '{"text": "Hello from curl!"}' \
  https://hooks.slack.com/services/T00/B00/xxxx
```

You should see a message appear in the channel you selected.

### Step 3 -- Add the URL to `.env`

Never hardcode webhook URLs in source code. Add it to your `.env` file
(already gitignored):

```bash
# .env
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T00/B00/xxxx
```

The variable is listed in `.env.example` as a reference.

### Step 4 -- Use in a flow

```python
import os

from dotenv import load_dotenv
from prefect import flow
from prefect.blocks.notifications import SlackWebhook
from pydantic import SecretStr

@flow(log_prints=True)
def slack_test() -> None:
    url = os.environ["SLACK_WEBHOOK_URL"]
    slack = SlackWebhook(url=SecretStr(url))
    slack.notify(body="Hello from Prefect!", subject="Test Notification")
    print("Notification sent")

if __name__ == "__main__":
    load_dotenv()
    slack_test()
```

Run it:

```bash
uv run python my_slack_test.py
```

### Step 5 -- Save as a block for reuse

Saving the block to the Prefect server means you never hardcode the URL again:

```python
import os
from pydantic import SecretStr
from prefect.blocks.notifications import SlackWebhook

slack = SlackWebhook(url=SecretStr(os.environ["SLACK_WEBHOOK_URL"]))
slack.save("prod-slack", overwrite=True)
print("Block saved!")
```

From now on, any flow can load it:

```python
slack = SlackWebhook.load("prod-slack")
slack.notify(body="Pipeline finished", subject="ETL Complete")
```

### Step 6 -- Use in flow hooks

Wire the saved block into lifecycle hooks for automatic alerting:

```python
from prefect import flow
from prefect.blocks.notifications import SlackWebhook

def on_completion(flow, flow_run, state):
    SlackWebhook.load("prod-slack").notify(
        body=f"Flow {flow_run.name!r} completed successfully.",
        subject="Flow Completed",
    )

def on_failure(flow, flow_run, state):
    SlackWebhook.load("prod-slack").notify(
        body=f"Flow {flow_run.name!r} failed: {state.message}",
        subject="Flow Failed",
    )

@flow(on_completion=[on_completion], on_failure=[on_failure], log_prints=True)
def my_pipeline() -> None:
    print("Doing work...")

if __name__ == "__main__":
    my_pipeline()
```

### Step 7 -- CustomWebhookNotificationBlock for generic webhooks

`CustomWebhookNotificationBlock` works with any HTTP endpoint -- Discord,
Teams, or a custom monitoring service:

```python
from prefect.blocks.notifications import CustomWebhookNotificationBlock

webhook = CustomWebhookNotificationBlock(
    name="discord-alerts",
    url="https://discord.com/api/webhooks/1234/abcd",
    method="POST",
    json_data={"content": "**{{subject}}**\n{{body}}"},
)
webhook.notify(body="All checks passed", subject="Quality Report")

# Save for reuse
webhook.save("discord-alerts", overwrite=True)
```

Template placeholders (`{{subject}}`, `{{body}}`, `{{name}}`) and custom
`secrets` keys are resolved automatically before the HTTP request is sent.

### What you learned

- Slack Incoming Webhooks provide a simple URL for posting messages
- `SlackWebhook` wraps the URL in a `SecretStr` and sends via `notify()`
- Saving blocks with `.save()` avoids hardcoding URLs in source code
- Flow hooks (`on_completion`, `on_failure`) automate notifications
- `CustomWebhookNotificationBlock` extends the same pattern to any HTTP
  endpoint with template resolution

**Related flows:** [Notification Blocks](flow-reference.md#notification-blocks),
[Webhook Notifications](flow-reference.md#webhook-notifications)
