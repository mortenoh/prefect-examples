"""DHIS2 Connection Block.

Custom Prefect block for DHIS2 credentials with SecretStr-based password
management and connection verification via a real API call.

Airflow equivalent: BaseHook.get_connection("dhis2_default") (DAG 110).
Prefect approach:    Custom Block subclass with methods and SecretStr.
"""

from __future__ import annotations

from typing import Any

from prefect import flow, task
from pydantic import BaseModel

from prefect_examples.dhis2 import (
    Dhis2ApiResponse,
    Dhis2Client,
    Dhis2Credentials,
    get_dhis2_credentials,
)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class ConnectionInfo(BaseModel):
    """Inspected connection details (password is never exposed)."""

    conn_type: str
    host: str
    username: str
    has_password: bool


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def get_connection_info(conn: Dhis2Credentials) -> ConnectionInfo:
    """Inspect a DHIS2 connection block.

    Args:
        conn: DHIS2 connection block.

    Returns:
        ConnectionInfo summary.
    """
    info = ConnectionInfo(
        conn_type="dhis2",
        host=conn.base_url,
        username=conn.username,
        has_password=len(conn.password.get_secret_value()) > 0,
    )
    print(f"Connection: {info.username}@{info.host}")
    return info


@task
def verify_connection(client: Dhis2Client, base_url: str) -> Dhis2ApiResponse:
    """Verify connectivity by fetching system/info from the DHIS2 API.

    Args:
        client: Authenticated DHIS2 API client.
        base_url: DHIS2 instance base URL (for display).

    Returns:
        Dhis2ApiResponse from the system/info endpoint.
    """
    data: dict[str, Any] = client.get_server_info()
    print(f"Verified: DHIS2 v{data.get('version', 'unknown')} at {base_url}")
    return Dhis2ApiResponse(endpoint="system/info", record_count=1)


@task
def fetch_org_unit_count(client: Dhis2Client) -> int:
    """Fetch org unit count to confirm API access.

    Args:
        client: Authenticated DHIS2 API client.

    Returns:
        Number of organisation units.
    """
    records = client.fetch_metadata("organisationUnits", fields="id")
    count = len(records)
    print(f"Organisation unit count: {count}")
    return count


@task
def display_connection(info: ConnectionInfo, org_unit_count: int) -> str:
    """Format a human-readable connection summary.

    Args:
        info: Connection info.
        org_unit_count: Number of org units fetched.

    Returns:
        Formatted summary string.
    """
    lines = [
        f"Type:      {info.conn_type}",
        f"Host:      {info.host}",
        f"User:      {info.username}",
        f"Org units: {org_unit_count}",
    ]
    summary = "\n".join(lines)
    print(summary)
    return summary


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="dhis2_connection", log_prints=True)
def dhis2_connection_flow() -> ConnectionInfo:
    """Demonstrate DHIS2 connection block and credential management.

    Returns:
        ConnectionInfo.
    """
    creds = get_dhis2_credentials()
    client = creds.get_client()
    info = get_connection_info(creds)
    verify_connection(client, creds.base_url)
    count = fetch_org_unit_count(client)
    display_connection(info, count)
    return info


if __name__ == "__main__":
    dhis2_connection_flow()
