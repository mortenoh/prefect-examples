"""107 -- DHIS2 Analytics Query.

Analytics API with dimension parameters, headers+rows response parsing,
and query parameter construction.

Airflow equivalent: DHIS2 data values / analytics (DAG 111).
Prefect approach:    Custom block auth, dimension query builder, tabular parsing.
"""

from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
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
RawAnalyticsResponse = _helpers.RawAnalyticsResponse
get_dhis2_connection = _helpers.get_dhis2_connection
get_dhis2_password = _helpers.get_dhis2_password
simulate_analytics = _helpers.simulate_analytics

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class AnalyticsQuery(BaseModel):
    """Describes an analytics API query."""

    data_elements: list[str]
    org_units: list[str]
    periods: list[str]


class AnalyticsRow(BaseModel):
    """A single parsed analytics row."""

    dx: str
    ou: str
    pe: str
    value: float


class AnalyticsReport(BaseModel):
    """Summary report for an analytics query."""

    query: AnalyticsQuery
    row_count: int
    data_element_counts: dict[str, int]
    value_range: tuple[float, float]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def build_query(
    data_elements: list[str] | None = None,
    org_units: list[str] | None = None,
    periods: list[str] | None = None,
) -> AnalyticsQuery:
    """Build an analytics query from dimension parameters.

    Args:
        data_elements: Data element UIDs.
        org_units: Org unit UIDs.
        periods: Period identifiers (e.g. "202301").

    Returns:
        AnalyticsQuery.
    """
    query = AnalyticsQuery(
        data_elements=data_elements or ["DE_001", "DE_002", "DE_003"],
        org_units=org_units or ["OU_001", "OU_002"],
        periods=periods or ["202301", "202302", "202303"],
    )
    print(f"Query: {len(query.data_elements)} DEs x {len(query.org_units)} OUs x {len(query.periods)} periods")
    return query


@task
def fetch_analytics(conn: Dhis2Connection, password: str, query: AnalyticsQuery) -> RawAnalyticsResponse:
    """Fetch analytics data from the DHIS2 API.

    In a real implementation::

        params = {
            "dimension": [
                f"dx:{';'.join(query.data_elements)}",
                f"ou:{';'.join(query.org_units)}",
                f"pe:{';'.join(query.periods)}",
            ]
        }
        response = httpx.get(f"{conn.base_url}/api/analytics",
                             auth=(conn.username, password), params=params)

    Args:
        conn: DHIS2 connection block.
        password: DHIS2 password.
        query: Analytics query.

    Returns:
        RawAnalyticsResponse.
    """
    _ = conn, password
    response = simulate_analytics(query.data_elements, query.org_units, query.periods)
    print(f"Fetched analytics: {len(response.rows)} rows")
    return response


@task
def parse_analytics(response: RawAnalyticsResponse) -> list[AnalyticsRow]:
    """Parse headers+rows analytics response into typed records.

    Maps column names from headers to row values.

    Args:
        response: Raw analytics response.

    Returns:
        List of AnalyticsRow.
    """
    header_names = [h["name"] for h in response.headers]
    rows: list[AnalyticsRow] = []
    for row_data in response.rows:
        record = dict(zip(header_names, row_data, strict=True))
        rows.append(
            AnalyticsRow(
                dx=record["dx"],
                ou=record["ou"],
                pe=record["pe"],
                value=float(record["value"]),
            )
        )
    print(f"Parsed {len(rows)} analytics rows")
    return rows


@task
def write_analytics_csv(rows: list[AnalyticsRow], output_dir: str) -> Path:
    """Write parsed analytics rows to CSV.

    Args:
        rows: Parsed analytics rows.
        output_dir: Output directory path.

    Returns:
        Path to the CSV file.
    """
    path = Path(output_dir) / "analytics.csv"
    fieldnames = list(AnalyticsRow.model_fields.keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r.model_dump())
    print(f"Wrote {len(rows)} analytics rows to {path}")
    return path


@task
def analytics_report(rows: list[AnalyticsRow], query: AnalyticsQuery) -> AnalyticsReport:
    """Build a summary report for analytics data.

    Args:
        rows: Parsed analytics rows.
        query: The original query.

    Returns:
        AnalyticsReport.
    """
    dx_counts: dict[str, int] = {}
    values: list[float] = []
    for r in rows:
        dx_counts[r.dx] = dx_counts.get(r.dx, 0) + 1
        values.append(r.value)
    value_range = (min(values), max(values)) if values else (0.0, 0.0)
    return AnalyticsReport(
        query=query,
        row_count=len(rows),
        data_element_counts=dx_counts,
        value_range=value_range,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="107_dhis2_analytics", log_prints=True)
def dhis2_analytics_flow(output_dir: str | None = None) -> AnalyticsReport:
    """Query DHIS2 analytics API and export results.

    Args:
        output_dir: Output directory. Uses temp dir if not provided.

    Returns:
        AnalyticsReport.
    """
    if output_dir is None:
        import tempfile

        output_dir = tempfile.mkdtemp(prefix="dhis2_analytics_")

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    conn = get_dhis2_connection()
    password = get_dhis2_password()

    query = build_query()
    response = fetch_analytics(conn, password, query)
    rows = parse_analytics(response)
    write_analytics_csv(rows, output_dir)
    report = analytics_report(rows, query)

    create_markdown_artifact(
        key="dhis2-analytics-report",
        markdown=(
            f"## Analytics Report\n\n"
            f"- Rows: {report.row_count}\n"
            f"- Value range: {report.value_range[0]}--{report.value_range[1]}\n"
            f"- Data elements: {report.data_element_counts}\n"
        ),
    )
    print(f"Analytics report: {report.row_count} rows, range {report.value_range}")
    return report


if __name__ == "__main__":
    dhis2_analytics_flow()
