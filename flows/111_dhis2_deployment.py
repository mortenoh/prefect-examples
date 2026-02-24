"""111 -- DHIS2 Deployment.

Demonstrate deploying a DHIS2 metadata sync flow with parameters and schedules.

Airflow equivalent: Scheduled DAG with Connections + Variables.
Prefect approach:    flow.serve() or flow.deploy() with blocks + parameters.

This flow bridges the deployment basics (037-039) with the DHIS2 integration
(101-110).  It shows how to build a deployment-ready flow that accepts
parameters, uses ``prefect.runtime`` for deployment-aware context, and loads
credentials from a block.

Deploy with flow.serve() (simplest -- runs in-process)::

    dhis2_deployment_flow.serve(
        name="dhis2-daily-sync",
        cron="0 6 * * *",
        parameters={"endpoints": ["organisationUnits", "dataElements"]},
    )

Deploy with flow.deploy() (production -- sends runs to a work pool)::

    dhis2_deployment_flow.deploy(
        name="dhis2-daily-sync",
        work_pool_name="my-pool",
        cron="0 6 * * *",
        parameters={"endpoints": ["organisationUnits", "dataElements"]},
    )

Declare in prefect.yaml (see repo root)::

    deployments:
      - name: dhis2-daily-sync
        entrypoint: flows/111_dhis2_deployment.py:dhis2_deployment_flow
        parameters:
          endpoints: [organisationUnits, dataElements, indicators]
        schedules:
          - cron: "0 6 * * *"
        work_pool:
          name: default
"""

from __future__ import annotations

from typing import Any

from prefect import flow, task
from prefect.runtime import deployment
from pydantic import BaseModel

from prefect_examples.dhis2 import Dhis2Connection, get_dhis2_connection

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class ServerInfo(BaseModel):
    """DHIS2 server version info."""

    version: str = "unknown"
    revision: str = ""
    server_date: str = ""


class EndpointSyncResult(BaseModel):
    """Result from syncing a single DHIS2 metadata endpoint."""

    endpoint: str
    record_count: int


class SyncReport(BaseModel):
    """Overall sync report across all endpoints."""

    deployment_name: str
    server_version: str
    endpoint_results: list[EndpointSyncResult]
    total_records: int
    summary_markdown: str


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def connect_and_verify(conn: Dhis2Connection) -> ServerInfo:
    """Verify DHIS2 connectivity and return server info as a typed model.

    Args:
        conn: DHIS2 connection block.

    Returns:
        ServerInfo with version details.
    """
    data: dict[str, Any] = conn.get_server_info()
    info = ServerInfo(
        version=data.get("version", "unknown"),
        revision=data.get("revision", ""),
        server_date=data.get("serverDate", ""),
    )
    print(f"Connected to DHIS2 {info.version}")
    return info


@task
def sync_endpoint(conn: Dhis2Connection, endpoint: str) -> EndpointSyncResult:
    """Fetch all records from a single DHIS2 metadata endpoint.

    Args:
        conn: DHIS2 connection block.
        endpoint: API endpoint name (e.g. "organisationUnits").

    Returns:
        EndpointSyncResult with endpoint name and record count.
    """
    records = conn.fetch_metadata(endpoint)
    print(f"Synced {len(records)} records from {endpoint}")
    return EndpointSyncResult(endpoint=endpoint, record_count=len(records))


@task
def build_sync_summary(
    server_info: ServerInfo,
    results: list[EndpointSyncResult],
) -> SyncReport:
    """Build a typed sync report with markdown summary.

    Uses ``prefect.runtime.deployment`` to include the deployment name when
    running inside a deployment (falls back to "local" for direct runs).

    Args:
        server_info: DHIS2 server version info.
        results: Sync results from each endpoint.

    Returns:
        SyncReport with endpoint results and markdown summary.
    """
    dep_name = deployment.name or "local"
    total = sum(r.record_count for r in results)

    lines = [f"# DHIS2 Metadata Sync ({dep_name})", ""]
    lines.append(f"Server version: {server_info.version}")
    lines.append("")
    lines.append("| Endpoint | Records |")
    lines.append("|----------|---------|")
    for r in results:
        lines.append(f"| {r.endpoint} | {r.record_count} |")
    lines.append(f"\n**Total:** {total} records")
    markdown = "\n".join(lines)

    return SyncReport(
        deployment_name=dep_name,
        server_version=server_info.version,
        endpoint_results=results,
        total_records=total,
        summary_markdown=markdown,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="111_dhis2_deployment", log_prints=True)
def dhis2_deployment_flow(
    endpoints: list[str] | None = None,
) -> SyncReport:
    """DHIS2 metadata sync -- deployment-ready with parameters.

    This flow is designed to be deployed via ``flow.serve()``,
    ``flow.deploy()``, or ``prefect.yaml``.  Default endpoints sync the
    three core metadata types; override via the *endpoints* parameter to
    target specific resources.

    Args:
        endpoints: List of DHIS2 metadata endpoint names to sync.
            Defaults to organisationUnits, dataElements, indicators.

    Returns:
        SyncReport with endpoint results and markdown summary.
    """
    if endpoints is None:
        endpoints = ["organisationUnits", "dataElements", "indicators"]

    conn = get_dhis2_connection()
    server_info = connect_and_verify(conn)

    results: list[EndpointSyncResult] = []
    for ep in endpoints:
        results.append(sync_endpoint(conn, ep))

    report = build_sync_summary(server_info, results)
    print(report.summary_markdown)
    return report


if __name__ == "__main__":
    dhis2_deployment_flow()
