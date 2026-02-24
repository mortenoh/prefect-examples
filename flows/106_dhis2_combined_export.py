"""106 -- DHIS2 Combined Export.

Parallel multi-endpoint fetch from the DHIS2 play server with shared block,
fan-in summary across heterogeneous outputs (CSV and JSON).

Airflow equivalent: DHIS2 combined parallel export (DAG 062).
Prefect approach:    .submit() for parallel tasks, fan-in combined report.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel

from prefect_examples.dhis2 import (
    Dhis2Connection,
    get_dhis2_connection,
)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class ExportResult(BaseModel):
    """Result from exporting a single endpoint."""

    endpoint: str
    record_count: int
    output_path: str
    format: str


class CombinedExportReport(BaseModel):
    """Summary across all exported endpoints."""

    results: list[ExportResult]
    total_records: int
    format_counts: dict[str, int]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def export_org_units(conn: Dhis2Connection, output_dir: str) -> ExportResult:
    """Fetch org units and export to CSV.

    Args:
        conn: DHIS2 connection block.
        output_dir: Output directory.

    Returns:
        ExportResult.
    """
    records = conn.fetch_metadata("organisationUnits")
    path = Path(output_dir) / "org_units.csv"
    if records:
        fieldnames = list(records[0].keys())
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in records:
                row = {k: str(v) for k, v in r.items()}
                writer.writerow(row)
    print(f"Exported {len(records)} org units to CSV")
    return ExportResult(endpoint="organisationUnits", record_count=len(records), output_path=str(path), format="csv")


@task
def export_data_elements(conn: Dhis2Connection, output_dir: str) -> ExportResult:
    """Fetch data elements and export to JSON.

    Args:
        conn: DHIS2 connection block.
        output_dir: Output directory.

    Returns:
        ExportResult.
    """
    records = conn.fetch_metadata("dataElements")
    path = Path(output_dir) / "data_elements.json"
    path.write_text(json.dumps(records, indent=2, default=str))
    print(f"Exported {len(records)} data elements to JSON")
    return ExportResult(endpoint="dataElements", record_count=len(records), output_path=str(path), format="json")


@task
def export_indicators(conn: Dhis2Connection, output_dir: str) -> ExportResult:
    """Fetch indicators and export to CSV.

    Args:
        conn: DHIS2 connection block.
        output_dir: Output directory.

    Returns:
        ExportResult.
    """
    records = conn.fetch_metadata("indicators")
    path = Path(output_dir) / "indicators.csv"
    if records:
        fieldnames = list(records[0].keys())
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in records:
                row = {k: str(v) for k, v in r.items()}
                writer.writerow(row)
    print(f"Exported {len(records)} indicators to CSV")
    return ExportResult(endpoint="indicators", record_count=len(records), output_path=str(path), format="csv")


@task
def combined_report(results: list[ExportResult]) -> CombinedExportReport:
    """Combine export results into a summary report.

    Args:
        results: Export results from all endpoints.

    Returns:
        CombinedExportReport.
    """
    total = sum(r.record_count for r in results)
    fmt_counts: dict[str, int] = {}
    for r in results:
        fmt_counts[r.format] = fmt_counts.get(r.format, 0) + 1
    return CombinedExportReport(results=results, total_records=total, format_counts=fmt_counts)


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="106_dhis2_combined_export", log_prints=True)
def dhis2_combined_export_flow(output_dir: str | None = None) -> CombinedExportReport:
    """Export org units, data elements, and indicators in parallel.

    Args:
        output_dir: Output directory. Uses temp dir if not provided.

    Returns:
        CombinedExportReport.
    """
    if output_dir is None:
        import tempfile

        output_dir = tempfile.mkdtemp(prefix="dhis2_export_")

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    conn = get_dhis2_connection()

    # Fan-out: 3 parallel endpoint exports
    future_ou = export_org_units.submit(conn, output_dir)
    future_de = export_data_elements.submit(conn, output_dir)
    future_ind = export_indicators.submit(conn, output_dir)

    result_ou = future_ou.result()
    result_de = future_de.result()
    result_ind = future_ind.result()

    report = combined_report([result_ou, result_de, result_ind])

    create_markdown_artifact(
        key="dhis2-combined-export",
        markdown=(
            f"## Combined Export Report\n\n"
            f"- Total records: {report.total_records}\n"
            f"- Endpoints: {len(report.results)}\n"
            f"- Formats: {report.format_counts}\n"
        ),
    )
    print(f"Combined export: {report.total_records} records across {len(report.results)} endpoints")
    return report


if __name__ == "__main__":
    dhis2_combined_export_flow()
