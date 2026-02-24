"""101 -- DHIS2 Connection Block.

Custom Prefect block for DHIS2 credentials with Secret-based password
management and connection verification.

Airflow equivalent: BaseHook.get_connection("dhis2_default") (DAG 110).
Prefect approach:    Custom Block subclass + Secret block for password.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from prefect import flow, task
from pydantic import BaseModel

# Import shared helpers
_spec = importlib.util.spec_from_file_location(
    "_dhis2_helpers",
    Path(__file__).resolve().parent / "_dhis2_helpers.py",
)
assert _spec and _spec.loader
_helpers = importlib.util.module_from_spec(_spec)
sys.modules.setdefault("_dhis2_helpers", _helpers)
_spec.loader.exec_module(_helpers)

Dhis2Connection = _helpers.Dhis2Connection
Dhis2ApiResponse = _helpers.Dhis2ApiResponse
get_dhis2_connection = _helpers.get_dhis2_connection
get_dhis2_password = _helpers.get_dhis2_password

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
    """Verify connectivity by simulating a ping to the DHIS2 API.

    Args:
        conn: DHIS2 connection block.
        password: DHIS2 password.

    Returns:
        Dhis2ApiResponse from the system/info endpoint.
    """
    # In a real implementation:
    # response = httpx.get(f"{conn.base_url}/api/{conn.api_version}/system/info",
    #                      auth=(conn.username, password))
    _ = conn, password
    result = Dhis2ApiResponse(endpoint="system/info", record_count=1, status_code=200)
    print(f"Verification: {result.endpoint} -> {result.status_code}")
    return result


@task
def display_connection(info: ConnectionInfo) -> str:
    """Format a human-readable connection summary.

    Args:
        info: Connection info.

    Returns:
        Formatted summary string.
    """
    lines = [
        f"Type:     {info.conn_type}",
        f"Host:     {info.host}",
        f"User:     {info.username}",
        f"Password: {info.masked_password}",
        f"API:      v{info.api_version}",
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
    display_connection(info)
    return info


if __name__ == "__main__":
    dhis2_connection_flow()
