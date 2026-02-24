# prefect-dhis2 Documentation

## Overview

`prefect-dhis2` is a Prefect integration package for
[DHIS2](https://dhis2.org), the open-source health information management
platform. It follows the same conventions as official Prefect integrations
(prefect-aws, prefect-gcp, etc.).

The package provides three main components:

- **`Dhis2Credentials`** -- a Prefect Block that stores connection details and
  appears in the Prefect UI.
- **`Dhis2Client`** -- an authenticated API client wrapping `httpx.Client`,
  scoped to the DHIS2 `/api` endpoint.
- **`Dhis2ApiResponse`** -- a Pydantic model for structured API response
  summaries.

### Architecture: Block vs Client

The design separates *credentials storage* from *API interaction*:

```
Dhis2Credentials (Block)       Dhis2Client
  - base_url                     - httpx.Client scoped to /api
  - username                     - get_server_info()
  - password (SecretStr)         - fetch_metadata()
  - get_client() ──────────>     - fetch_analytics()
```

The Block handles persistence (save/load, UI rendering, secret masking).
The Client handles HTTP communication. This mirrors how `prefect-aws` separates
`AwsCredentials` from `S3Client`.

## Installation

```bash
pip install prefect-dhis2
```

Or for development:

```bash
cd packages/prefect-dhis2
pip install -e ".[dev]"
```

### Block registration

The `prefect.collections` entry point means the block auto-registers when the
package is installed. For manual registration:

```bash
prefect block register -m prefect_dhis2
```

After registration, `Dhis2 Credentials` appears in the Prefect UI under
**Blocks > Add Block**.

## Dhis2Credentials Block

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_url` | `str` | `https://play.im.dhis2.org/dev` | DHIS2 instance base URL |
| `username` | `str` | `admin` | DHIS2 username |
| `password` | `SecretStr` | `district` | DHIS2 password (stored encrypted) |

### Creating credentials

```python
from prefect_dhis2 import Dhis2Credentials

# Inline (not persisted)
creds = Dhis2Credentials(
    base_url="https://play.im.dhis2.org/dev",
    username="admin",
    password="district",
)

# Save to Prefect server for reuse
creds.save("dhis2", overwrite=True)
```

### Loading credentials

```python
from prefect_dhis2 import Dhis2Credentials

creds = Dhis2Credentials.load("dhis2")
```

### Getting a client

```python
client = creds.get_client()
# ... use client ...
client.close()

# Or as a context manager:
with creds.get_client() as client:
    info = client.get_server_info()
```

## Dhis2Client API

All methods raise `httpx.HTTPStatusError` on non-2xx responses.

### `get_server_info() -> dict[str, Any]`

Fetches `/api/system/info` -- returns server version, revision, build time,
and other system metadata.

```python
with creds.get_client() as client:
    info = client.get_server_info()
    print(f"DHIS2 version: {info['version']}")
```

### `fetch_metadata(endpoint, fields=":owner") -> list[dict[str, Any]]`

Fetches all records from a DHIS2 metadata endpoint with paging disabled.

**Parameters:**

- `endpoint` -- API endpoint name (e.g. `"organisationUnits"`,
  `"dataElements"`, `"indicators"`)
- `fields` -- DHIS2 fields parameter (default `":owner"`)

```python
with creds.get_client() as client:
    orgs = client.fetch_metadata("organisationUnits", fields="id,displayName,level")
    print(f"Found {len(orgs)} org units")

    elements = client.fetch_metadata("dataElements", fields="id,displayName,valueType")
    print(f"Found {len(elements)} data elements")
```

### `fetch_analytics(dimension, filter_param=None) -> dict[str, Any]`

Fetches analytics data with dimension and optional filter parameters.

**Parameters:**

- `dimension` -- List of dimension parameters
  (e.g. `["dx:uid1;uid2", "ou:uidA"]`)
- `filter_param` -- Optional filter parameter
  (e.g. `"pe:LAST_4_QUARTERS"`)

```python
with creds.get_client() as client:
    data = client.fetch_analytics(
        dimension=["dx:fbfJHSPpUQD", "ou:ImspTQPwCqd"],
        filter_param="pe:LAST_12_MONTHS",
    )
    print(f"Headers: {data['headers']}")
    print(f"Rows: {len(data['rows'])}")
```

### `close()`

Closes the underlying `httpx.Client`. Called automatically when used as a
context manager.

### Pickling support

`Dhis2Client` implements `__reduce__` so Prefect can pickle and hash it for
cache key computation:

```python
import pickle
client = creds.get_client()
restored = pickle.loads(pickle.dumps(client))
```

## Dhis2ApiResponse Model

A Pydantic model for structured API response summaries.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `endpoint` | `str` | required | The API endpoint that was called |
| `record_count` | `int` | required | Number of records returned |
| `status_code` | `int` | `200` | HTTP status code |

```python
from prefect_dhis2 import Dhis2ApiResponse

resp = Dhis2ApiResponse(endpoint="/api/organisationUnits", record_count=1342)
print(resp.model_dump_json())
```

## get_dhis2_credentials() helper

A convenience function that attempts to load a saved block named `"dhis2"` and
falls back to default play-server credentials if loading fails.

```python
from prefect_dhis2 import get_dhis2_credentials

creds = get_dhis2_credentials()
```

This is useful for flows that should work both locally (with defaults) and in
deployed environments (with saved blocks).

## OPERAND_PATTERN

A compiled regex matching DHIS2 data element operand references in the format
`#{...}`:

```python
from prefect_dhis2 import OPERAND_PATTERN

text = "Value is #{fbfJHSPpUQD.pq2XI5kz2BY} and #{cYeuwXTCPkU}"
matches = OPERAND_PATTERN.findall(text)
# ['#{fbfJHSPpUQD.pq2XI5kz2BY}', '#{cYeuwXTCPkU}']
```

## Configuration strategies

### 1. Inline credentials (development)

```python
creds = Dhis2Credentials(
    base_url="https://play.im.dhis2.org/dev",
    username="admin",
    password="district",
)
```

### 2. Saved block (production)

```python
# One-time setup:
creds = Dhis2Credentials(base_url="https://prod.dhis2.org", ...)
creds.save("dhis2-prod")

# In flows:
creds = Dhis2Credentials.load("dhis2-prod")
```

### 3. Graceful fallback (portable flows)

```python
creds = get_dhis2_credentials()  # saved block or defaults
```

## Testing patterns

### Mocking the client

```python
from unittest.mock import MagicMock
from prefect_dhis2 import Dhis2Client

mock_client = MagicMock(spec=Dhis2Client)
mock_client.get_server_info.return_value = {"version": "2.41"}
mock_client.fetch_metadata.return_value = [{"id": "abc"}]
```

### Mocking credentials

```python
from unittest.mock import MagicMock, patch
from prefect_dhis2 import Dhis2Credentials

mock_creds = MagicMock(spec=Dhis2Credentials)
mock_creds.get_client.return_value = mock_client

with patch("your_module.get_dhis2_credentials", return_value=mock_creds):
    # your flow code runs with mocked credentials
    pass
```

## Comparison with Airflow

| Concept | Airflow | Prefect (this package) |
|---------|---------|----------------------|
| Store credentials | `Connection("dhis2_default")` | `Dhis2Credentials.save("dhis2")` |
| Load credentials | `BaseHook.get_connection(...)` | `Dhis2Credentials.load("dhis2")` |
| UI management | Admin > Connections | Blocks > Dhis2 Credentials |
| Secret masking | Connection `password` field | `SecretStr` (Pydantic) |
| Auto-discovery | Provider packages | `prefect.collections` entry point |
