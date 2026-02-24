"""096 -- Parallel Multi-Endpoint Export.

Parallel independent endpoint processing with heterogeneous output
formats (CSV + JSON), fan-in summary across all endpoints.

Airflow equivalent: DHIS2 combined parallel export (DAG 062).
Prefect approach:    Fetch 3 endpoints in parallel via .submit(), fan-in
                     combine, and produce summary report.
"""

import csv
import json
import time
from pathlib import Path

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class EndpointResult(BaseModel):
    """Result from processing a single endpoint."""

    endpoint: str
    record_count: int
    output_format: str
    output_path: str


class ExportSummary(BaseModel):
    """Summary across all exported endpoints."""

    endpoints: list[EndpointResult]
    total_records: int
    format_counts: dict[str, int]
    duration_seconds: float


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def fetch_endpoint_a(output_dir: str) -> EndpointResult:
    """Simulate fetching endpoint A and writing CSV output.

    Args:
        output_dir: Output directory path.

    Returns:
        EndpointResult.
    """
    records = [{"id": i, "name": f"org_unit_{i}", "level": (i % 4) + 1} for i in range(1, 16)]
    path = Path(output_dir) / "endpoint_a.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "level"])
        writer.writeheader()
        writer.writerows(records)
    print(f"Endpoint A: wrote {len(records)} records to CSV")
    return EndpointResult(
        endpoint="org_units",
        record_count=len(records),
        output_format="csv",
        output_path=str(path),
    )


@task
def fetch_endpoint_b(output_dir: str) -> EndpointResult:
    """Simulate fetching endpoint B and writing JSON output.

    Args:
        output_dir: Output directory path.

    Returns:
        EndpointResult.
    """
    records = [
        {"id": f"DE_{i:03d}", "name": f"data_element_{i}", "category": ["disease", "nutrition", "service"][i % 3]}
        for i in range(1, 21)
    ]
    path = Path(output_dir) / "endpoint_b.json"
    path.write_text(json.dumps(records, indent=2))
    print(f"Endpoint B: wrote {len(records)} records to JSON")
    return EndpointResult(
        endpoint="data_elements",
        record_count=len(records),
        output_format="json",
        output_path=str(path),
    )


@task
def fetch_endpoint_c(output_dir: str) -> EndpointResult:
    """Simulate fetching endpoint C and writing CSV output.

    Args:
        output_dir: Output directory path.

    Returns:
        EndpointResult.
    """
    records = [
        {"id": f"IND_{i:03d}", "name": f"indicator_{i}", "numerator": f"#{{{i}a}}", "denominator": f"#{{{i}b}}"}
        for i in range(1, 11)
    ]
    path = Path(output_dir) / "endpoint_c.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "numerator", "denominator"])
        writer.writeheader()
        writer.writerows(records)
    print(f"Endpoint C: wrote {len(records)} records to CSV")
    return EndpointResult(
        endpoint="indicators",
        record_count=len(records),
        output_format="csv",
        output_path=str(path),
    )


@task
def combine_results(results: list[EndpointResult], duration: float) -> ExportSummary:
    """Combine results from all endpoints.

    Args:
        results: Endpoint results.
        duration: Total processing duration in seconds.

    Returns:
        ExportSummary.
    """
    total = sum(r.record_count for r in results)
    format_counts: dict[str, int] = {}
    for r in results:
        format_counts[r.output_format] = format_counts.get(r.output_format, 0) + 1

    return ExportSummary(
        endpoints=results,
        total_records=total,
        format_counts=format_counts,
        duration_seconds=round(duration, 4),
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="096_parallel_export", log_prints=True)
def parallel_export_flow(output_dir: str | None = None) -> ExportSummary:
    """Run the parallel multi-endpoint export pipeline.

    Args:
        output_dir: Output directory. Uses temp dir if not provided.

    Returns:
        ExportSummary.
    """
    if output_dir is None:
        import tempfile

        output_dir = tempfile.mkdtemp(prefix="export_")

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    start = time.monotonic()

    # Fan-out: 3 parallel endpoint fetches
    future_a = fetch_endpoint_a.submit(output_dir)
    future_b = fetch_endpoint_b.submit(output_dir)
    future_c = fetch_endpoint_c.submit(output_dir)

    result_a = future_a.result()
    result_b = future_b.result()
    result_c = future_c.result()

    duration = time.monotonic() - start

    summary = combine_results([result_a, result_b, result_c], duration)
    print(f"Export complete: {summary.total_records} records across {len(summary.endpoints)} endpoints")
    return summary


if __name__ == "__main__":
    parallel_export_flow()
