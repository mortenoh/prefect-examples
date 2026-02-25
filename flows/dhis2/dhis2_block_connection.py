"""DHIS2 Block-Based Connection with Multi-Instance Support.

Loads a named DHIS2 credentials block so different instances (play, staging,
production) can be targeted at runtime via the ``instance`` parameter.

Airflow equivalent: Multiple connections per conn_id.
Prefect approach:    One Block type, multiple saved instances by name.
"""

from __future__ import annotations

from typing import Any

from dotenv import load_dotenv
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

    instance: str
    conn_type: str
    host: str
    username: str
    has_password: bool


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def get_connection_info(conn: Dhis2Credentials, instance: str) -> ConnectionInfo:
    """Inspect a DHIS2 connection block.

    Args:
        conn: DHIS2 connection block.
        instance: Block name that was loaded.

    Returns:
        ConnectionInfo summary.
    """
    info = ConnectionInfo(
        instance=instance,
        conn_type="dhis2",
        host=conn.base_url,
        username=conn.username,
        has_password=len(conn.password.get_secret_value()) > 0,
    )
    print(f"Connection [{instance}]: {info.username}@{info.host}")
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
        f"Instance:  {info.instance}",
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


@flow(name="dhis2_block_connection", log_prints=True)
def dhis2_block_connection_flow(instance: str = "dhis2") -> ConnectionInfo:
    """Demonstrate block-based DHIS2 connection with multi-instance support.

    Args:
        instance: Name of the Prefect block to load.

    Returns:
        ConnectionInfo.
    """
    creds = get_dhis2_credentials(instance)
    client = creds.get_client()
    info = get_connection_info(creds, instance)
    verify_connection(client, creds.base_url)
    count = fetch_org_unit_count(client)
    display_connection(info, count)
    return info


if __name__ == "__main__":
    load_dotenv()
    dhis2_block_connection_flow()
