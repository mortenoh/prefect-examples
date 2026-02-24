"""101 -- DHIS2 Connection Block.

Custom Prefect block for DHIS2 credentials with Secret-based password
management and connection verification via a real API call.

Airflow equivalent: BaseHook.get_connection("dhis2_default") (DAG 110).
Prefect approach:    Custom Block subclass + Secret block for password.
"""

from __future__ import annotations

from typing import Any

import httpx
from prefect import flow, task
from pydantic import BaseModel

from prefect_examples.dhis2 import (
    Dhis2ApiResponse,
    Dhis2Connection,
    fetch_metadata,
    get_dhis2_connection,
    get_dhis2_password,
)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class ConnectionInfo(BaseModel):
    """Inspected connection details with masked credentials."""

    conn_type: str
    host: str
    username: str
    masked_password: str
    api_version: str


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def get_connection_info(conn: Dhis2Connection, password: str) -> ConnectionInfo:
    """Inspect a DHIS2 connection and mask the password.

    Args:
        conn: DHIS2 connection block.
        password: Plain-text password.

    Returns:
        ConnectionInfo with masked password.
    """
    masked = password[:1] + "*" * (len(password) - 2) + password[-1:] if len(password) > 2 else "***"
    info = ConnectionInfo(
        conn_type="dhis2",
        host=conn.base_url,
        username=conn.username,
        masked_password=masked,
        api_version=conn.api_version,
    )
    print(f"Connection: {info.username}@{info.host} (v{info.api_version})")
    return info


@task
def verify_connection(conn: Dhis2Connection, password: str) -> Dhis2ApiResponse:
    """Verify connectivity by fetching system/info from the DHIS2 API.

    Args:
        conn: DHIS2 connection block.
        password: DHIS2 password.

    Returns:
        Dhis2ApiResponse from the system/info endpoint.
    """
    url = f"{conn.base_url}/api/system/info"
    resp = httpx.get(url, auth=(conn.username, password), timeout=30)
    resp.raise_for_status()
    data: dict[str, Any] = resp.json()
    print(f"Verified: DHIS2 v{data.get('version', 'unknown')} at {conn.base_url}")
    return Dhis2ApiResponse(endpoint="system/info", record_count=1, status_code=resp.status_code)


@task
def fetch_org_unit_count(conn: Dhis2Connection, password: str) -> int:
    """Fetch org unit count to confirm API access.

    Args:
        conn: DHIS2 connection block.
        password: DHIS2 password.

    Returns:
        Number of organisation units.
    """
    records = fetch_metadata(conn, "organisationUnits", password, fields="id")
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
        f"Password:  {info.masked_password}",
        f"API:       v{info.api_version}",
        f"Org units: {org_unit_count}",
    ]
    summary = "\n".join(lines)
    print(summary)
    return summary


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="101_dhis2_connection", log_prints=True)
def dhis2_connection_flow() -> ConnectionInfo:
    """Demonstrate DHIS2 connection block and credential management.

    Returns:
        ConnectionInfo.
    """
    conn = get_dhis2_connection()
    password = get_dhis2_password()
    info = get_connection_info(conn, password)
    verify_connection(conn, password)
    count = fetch_org_unit_count(conn, password)
    display_connection(info, count)
    return info


if __name__ == "__main__":
    dhis2_connection_flow()
