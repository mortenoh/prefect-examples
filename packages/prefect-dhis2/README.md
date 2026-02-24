# prefect-dhis2

Prefect integration for [DHIS2](https://dhis2.org) -- the open-source health
information management platform used in 100+ countries.

Provides a `Dhis2Credentials` block for storing connection details and a
`Dhis2Client` for authenticated API access. After installation the block
appears automatically in the Prefect UI.

## Installation

```bash
pip install prefect-dhis2
```

## Quick start

### Create and use credentials inline

```python
from prefect_dhis2 import Dhis2Credentials

creds = Dhis2Credentials(
    base_url="https://play.im.dhis2.org/dev",
    username="admin",
    password="district",
)

client = creds.get_client()
info = client.get_server_info()
print(info["version"])
client.close()
```

### Save a block for reuse

```python
from prefect_dhis2 import Dhis2Credentials

creds = Dhis2Credentials(
    base_url="https://play.im.dhis2.org/dev",
    username="admin",
    password="district",
)
creds.save("dhis2", overwrite=True)
```

### Load a saved block

```python
from prefect_dhis2 import Dhis2Credentials

creds = Dhis2Credentials.load("dhis2")
client = creds.get_client()
```

### Use the helper function (graceful fallback)

```python
from prefect_dhis2 import get_dhis2_credentials

creds = get_dhis2_credentials()  # loads saved block or falls back to defaults
client = creds.get_client()
```

### Client as a context manager

```python
from prefect_dhis2 import get_dhis2_credentials

creds = get_dhis2_credentials()
with creds.get_client() as client:
    orgs = client.fetch_metadata("organisationUnits", fields="id,displayName")
    print(f"Found {len(orgs)} organisation units")
```

## API reference

### `Dhis2Credentials` (Block)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_url` | `str` | `https://play.im.dhis2.org/dev` | DHIS2 instance base URL |
| `username` | `str` | `admin` | DHIS2 username |
| `password` | `SecretStr` | `district` | DHIS2 password |

**Methods:** `get_client() -> Dhis2Client`

### `Dhis2Client`

| Method | Description |
|--------|-------------|
| `get_server_info()` | Fetch `/api/system/info` |
| `fetch_metadata(endpoint, fields)` | Fetch all records from a metadata endpoint |
| `fetch_analytics(dimension, filter_param)` | Fetch analytics data |
| `close()` | Close the underlying HTTP client |

### `Dhis2ApiResponse` (Pydantic model)

| Field | Type | Default |
|-------|------|---------|
| `endpoint` | `str` | required |
| `record_count` | `int` | required |
| `status_code` | `int` | `200` |

### `get_dhis2_credentials() -> Dhis2Credentials`

Loads the saved `"dhis2"` block or falls back to default play-server credentials.

### `OPERAND_PATTERN`

Compiled regex matching DHIS2 data element operand references (`#{...}`).

## Block registration

After `pip install prefect-dhis2`, the block auto-registers via the
`prefect.collections` entry point. You can also register manually:

```bash
prefect block register -m prefect_dhis2
```

## Development

```bash
cd packages/prefect-dhis2
pip install -e ".[dev]"
pytest
```

## License

Apache-2.0
