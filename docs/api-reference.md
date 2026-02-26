# API Reference -- Prefect HTTP API

The Prefect server exposes a REST API that backs every operation in the UI,
CLI, and Python SDK. This page documents the most common endpoints and
patterns for interacting with it directly.

---

## Overview

| Detail       | Value                                  |
| ------------ | -------------------------------------- |
| Base URL     | `http://localhost:4200/api`            |
| Protocol     | HTTP / JSON                            |
| Auth (OSS)   | None                                   |
| Auth (Cloud) | Bearer token via `PREFECT_API_KEY`     |

Set the base URL with the `PREFECT_API_URL` environment variable or the
`prefect config set` command:

```bash
export PREFECT_API_URL="http://localhost:4200/api"
# or
prefect config set PREFECT_API_URL="http://localhost:4200/api"
```

---

## Authentication

**Prefect OSS** -- no authentication is required. The API is open on the
configured host/port.

**Prefect Cloud** -- every request must include an `Authorization` header:

```
Authorization: Bearer <PREFECT_API_KEY>
```

```bash
curl -s https://api.prefect.cloud/api/accounts/<ACCOUNT_ID>/workspaces/<WORKSPACE_ID>/flows \
  -H "Authorization: Bearer $PREFECT_API_KEY"
```

---

## Health check

```bash
curl -s http://localhost:4200/api/health
# 200 OK  -- returns true when the server is ready
```

---

## Endpoint groups

All request/response bodies are JSON. For POST endpoints that filter or
list resources, send a JSON body with filter, sort, limit, and offset
fields.

### Flows

**List flows**

```bash
curl -s http://localhost:4200/api/flows/filter \
  -H "Content-Type: application/json" \
  -d '{"limit": 10}'
```

**Read a flow**

```bash
curl -s http://localhost:4200/api/flows/<FLOW_ID>
```

### Deployments

**List deployments**

```bash
curl -s http://localhost:4200/api/deployments/filter \
  -H "Content-Type: application/json" \
  -d '{"limit": 10}'
```

**Create a deployment**

```bash
curl -s -X POST http://localhost:4200/api/deployments/ \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-deployment",
    "flow_id": "<FLOW_ID>",
    "work_pool_name": "default"
  }'
```

**Read a deployment**

```bash
curl -s http://localhost:4200/api/deployments/<DEPLOYMENT_ID>
```

**Read a deployment by name**

Look up a deployment using the flow name and deployment name instead of an ID:

```bash
curl -s http://localhost:4200/api/deployments/name/<FLOW_NAME>/<DEPLOYMENT_NAME>
```

Example:

```bash
curl -s http://localhost:4200/api/deployments/name/dhis2_org_units/dhis2-org-units
```

**Trigger a deployment run**

```bash
curl -s -X POST http://localhost:4200/api/deployments/<DEPLOYMENT_ID>/create_flow_run \
  -H "Content-Type: application/json" \
  -d '{}'
```

**Trigger a deployment run with parameters**

```bash
curl -s -X POST http://localhost:4200/api/deployments/<DEPLOYMENT_ID>/create_flow_run \
  -H "Content-Type: application/json" \
  -d '{"parameters": {"output_dir": "/tmp/results", "batch_size": 500}}'
```

**Trigger by name (two-step)**

The `create_flow_run` endpoint requires a deployment ID. Combine the name lookup
with the trigger in a single command:

```bash
DEPLOY_ID=$(curl -s http://localhost:4200/api/deployments/name/dhis2_org_units/dhis2-org-units \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")

curl -s -X POST "http://localhost:4200/api/deployments/$DEPLOY_ID/create_flow_run" \
  -H "Content-Type: application/json" \
  -d '{}'
```

**Pause / resume a deployment**

```bash
# Pause
curl -s -X POST http://localhost:4200/api/deployments/<DEPLOYMENT_ID>/set_schedule_inactive

# Resume
curl -s -X POST http://localhost:4200/api/deployments/<DEPLOYMENT_ID>/set_schedule_active
```

**Set a schedule**

```bash
curl -s -X POST http://localhost:4200/api/deployments/<DEPLOYMENT_ID>/schedule \
  -H "Content-Type: application/json" \
  -d '{
    "schedule": {"cron": "0 8 * * *", "timezone": "UTC"}
  }'
```

### Flow runs

**List flow runs**

```bash
curl -s http://localhost:4200/api/flow_runs/filter \
  -H "Content-Type: application/json" \
  -d '{"limit": 5, "sort": "EXPECTED_START_TIME_DESC"}'
```

**Read a flow run**

```bash
curl -s http://localhost:4200/api/flow_runs/<FLOW_RUN_ID>
```

**Create (trigger) a flow run**

```bash
curl -s -X POST http://localhost:4200/api/flow_runs/ \
  -H "Content-Type: application/json" \
  -d '{
    "flow_id": "<FLOW_ID>",
    "deployment_id": "<DEPLOYMENT_ID>",
    "state": {"type": "SCHEDULED"}
  }'
```

**Cancel a flow run**

```bash
curl -s -X POST http://localhost:4200/api/flow_runs/<FLOW_RUN_ID>/set_state \
  -H "Content-Type: application/json" \
  -d '{"state": {"type": "CANCELLING"}}'
```

**Delete a flow run**

```bash
curl -s -X DELETE http://localhost:4200/api/flow_runs/<FLOW_RUN_ID>
```

### Work pools

**List work pools**

```bash
curl -s http://localhost:4200/api/work_pools/filter \
  -H "Content-Type: application/json" \
  -d '{}'
```

**Create a work pool**

```bash
curl -s -X POST http://localhost:4200/api/work_pools/ \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-pool",
    "type": "process"
  }'
```

**Read a work pool**

```bash
curl -s http://localhost:4200/api/work_pools/<WORK_POOL_NAME>
```

**Update a work pool**

```bash
curl -s -X PATCH http://localhost:4200/api/work_pools/<WORK_POOL_NAME> \
  -H "Content-Type: application/json" \
  -d '{"is_paused": true}'
```

**Delete a work pool**

```bash
curl -s -X DELETE http://localhost:4200/api/work_pools/<WORK_POOL_NAME>
```

**List workers in a pool**

```bash
curl -s http://localhost:4200/api/work_pools/<WORK_POOL_NAME>/workers/filter \
  -H "Content-Type: application/json" \
  -d '{}'
```

### Blocks

**List block types**

```bash
curl -s http://localhost:4200/api/block_types/filter \
  -H "Content-Type: application/json" \
  -d '{}'
```

**List block documents**

```bash
curl -s http://localhost:4200/api/block_documents/filter \
  -H "Content-Type: application/json" \
  -d '{"include_secrets": false}'
```

**Read a block document**

```bash
curl -s http://localhost:4200/api/block_documents/<BLOCK_DOCUMENT_ID>
```

**Create a block document**

```bash
curl -s -X POST http://localhost:4200/api/block_documents/ \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-secret",
    "block_type_id": "<BLOCK_TYPE_ID>",
    "data": {"value": "supersecret"}
  }'
```

**Delete a block document**

```bash
curl -s -X DELETE http://localhost:4200/api/block_documents/<BLOCK_DOCUMENT_ID>
```

### Automations

**List automations**

```bash
curl -s http://localhost:4200/api/automations/filter \
  -H "Content-Type: application/json" \
  -d '{}'
```

**Create an automation**

```bash
curl -s -X POST http://localhost:4200/api/automations/ \
  -H "Content-Type: application/json" \
  -d '{
    "name": "cancel-stuck-runs",
    "trigger": {
      "type": "event",
      "expect": ["prefect.flow-run.Failed"],
      "posture": "Reactive",
      "threshold": 1,
      "within": 0
    },
    "actions": [
      {"type": "cancel-flow-run"}
    ]
  }'
```

**Read an automation**

```bash
curl -s http://localhost:4200/api/automations/<AUTOMATION_ID>
```

**Update an automation**

```bash
curl -s -X PUT http://localhost:4200/api/automations/<AUTOMATION_ID> \
  -H "Content-Type: application/json" \
  -d '{ ... }'
```

**Delete an automation**

```bash
curl -s -X DELETE http://localhost:4200/api/automations/<AUTOMATION_ID>
```

**Pause / resume an automation**

```bash
# Pause
curl -s -X PATCH http://localhost:4200/api/automations/<AUTOMATION_ID> \
  -H "Content-Type: application/json" \
  -d '{"enabled": false}'

# Resume
curl -s -X PATCH http://localhost:4200/api/automations/<AUTOMATION_ID> \
  -H "Content-Type: application/json" \
  -d '{"enabled": true}'
```

### Events

**List events**

```bash
curl -s http://localhost:4200/api/events/filter \
  -H "Content-Type: application/json" \
  -d '{
    "filter": {
      "occurred": {
        "since_": "2024-01-01T00:00:00Z",
        "until_": "2024-12-31T23:59:59Z"
      }
    },
    "limit": 50
  }'
```

---

## Pagination and filtering

List/filter endpoints use a POST body with these common fields:

| Field    | Type   | Description                          |
| -------- | ------ | ------------------------------------ |
| `limit`  | int    | Max items to return (default varies) |
| `offset` | int    | Number of items to skip              |
| `sort`   | string | Sort key, e.g. `CREATED_DESC`        |
| `filter` | object | Nested filter conditions (see below) |

Filter objects are nested by resource. Example -- flow runs in a
`COMPLETED` state:

```json
{
  "flow_runs": {
    "state": {
      "type": {
        "any_": ["COMPLETED"]
      }
    }
  },
  "limit": 20,
  "offset": 0
}
```

Page through results by incrementing `offset` by `limit` until fewer than
`limit` items are returned.

---

## Common response codes

| Code | Meaning               | Typical cause                               |
| ---- | --------------------- | ------------------------------------------- |
| 200  | OK                    | Successful read or filter                   |
| 201  | Created               | Resource created (deployment, block, etc.)   |
| 204  | No Content            | Successful delete or update with no body     |
| 404  | Not Found             | ID does not exist                           |
| 409  | Conflict              | Duplicate name or unique constraint violated |
| 422  | Unprocessable Entity  | Validation error in the request body         |

---

## Python client

The `prefect` SDK and CLI use this REST API under the hood. For scripted
access outside of a flow context, `httpx` works well:

```python
import httpx

API = "http://localhost:4200/api"

# List flows
response = httpx.post(f"{API}/flows/filter", json={"limit": 5})
response.raise_for_status()

for flow in response.json():
    print(flow["name"], flow["id"])
```

For Prefect Cloud, add the authorization header:

```python
headers = {"Authorization": f"Bearer {api_key}"}
response = httpx.post(f"{API}/flows/filter", json={"limit": 5}, headers=headers)
```

---

## Airflow comparison

| Capability          | Airflow stable REST API         | Prefect REST API                      |
| ------------------- | ------------------------------- | ------------------------------------- |
| Auth                | Basic / Kerberos / session      | None (OSS) or Bearer token (Cloud)    |
| Trigger a DAG / run | `POST /dags/{id}/dagRuns`       | `POST /deployments/{id}/create_flow_run` |
| List runs           | `GET /dags/{id}/dagRuns`        | `POST /flow_runs/filter`              |
| Cancel a run        | `PATCH /dags/{id}/dagRuns/{id}` | `POST /flow_runs/{id}/set_state`      |
| Filter pattern      | Query-string params             | JSON body with nested filter objects  |
| Pagination          | `limit` + `offset` query params | `limit` + `offset` in JSON body      |
| Schema docs         | Swagger / OpenAPI at `/api/v1/ui` | OpenAPI schema at `/api/docs`       |

---

## Official documentation

Full schema and interactive docs are available at:

- **Self-hosted**: `http://localhost:4200/docs` (Swagger UI served by the Prefect server)
- **Prefect docs**: <https://docs.prefect.io/latest/api-ref/rest-api/>
