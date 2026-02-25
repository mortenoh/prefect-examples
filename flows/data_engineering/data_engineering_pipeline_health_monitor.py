"""Pipeline Health Monitor.

Meta-monitoring / watchdog pattern: a flow that checks the health of other
pipelines' outputs by verifying file existence, freshness, row counts, and
value ranges.

Airflow equivalent: Pipeline health check (DAG 076).
Prefect approach:    Health check registry with @task dispatchers, worst-status-wins
                     aggregation.
"""

import time
from pathlib import Path
from typing import Any

from prefect import flow, task
from prefect.artifacts import create_table_artifact
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class HealthCheck(BaseModel):
    """Definition of a health check."""

    name: str
    check_type: str
    target: str = ""
    params: dict[str, Any] = {}


class HealthCheckResult(BaseModel):
    """Result of a single health check."""

    check_name: str
    status: str  # healthy, degraded, critical
    details: str


class PipelineHealthReport(BaseModel):
    """Aggregated health report for a pipeline."""

    pipeline_name: str
    overall_status: str
    healthy_count: int
    degraded_count: int
    critical_count: int
    results: list[HealthCheckResult]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def check_file_exists(path: str) -> HealthCheckResult:
    """Check that a file exists.

    Args:
        path: File path to check.

    Returns:
        HealthCheckResult.
    """
    exists = Path(path).exists()
    return HealthCheckResult(
        check_name=f"file_exists:{Path(path).name}",
        status="healthy" if exists else "critical",
        details=f"File {'exists' if exists else 'missing'}: {path}",
    )


@task
def check_file_freshness(path: str, max_age_seconds: float = 3600) -> HealthCheckResult:
    """Check that a file was modified within the allowed age.

    Args:
        path: File path to check.
        max_age_seconds: Maximum allowed age in seconds.

    Returns:
        HealthCheckResult.
    """
    p = Path(path)
    if not p.exists():
        return HealthCheckResult(
            check_name=f"freshness:{p.name}",
            status="critical",
            details=f"File not found: {path}",
        )
    age = time.time() - p.stat().st_mtime
    if age <= max_age_seconds:
        status = "healthy"
    elif age <= max_age_seconds * 2:
        status = "degraded"
    else:
        status = "critical"
    return HealthCheckResult(
        check_name=f"freshness:{p.name}",
        status=status,
        details=f"File age: {age:.0f}s (max: {max_age_seconds:.0f}s)",
    )


@task
def check_row_count(data: list[dict[str, Any]], min_rows: int = 0, max_rows: int = 1000000) -> HealthCheckResult:
    """Check that data row count is within bounds.

    Args:
        data: Dataset rows.
        min_rows: Minimum expected rows.
        max_rows: Maximum expected rows.

    Returns:
        HealthCheckResult.
    """
    count = len(data)
    if min_rows <= count <= max_rows:
        status = "healthy"
    elif count < min_rows:
        status = "critical"
    else:
        status = "degraded"
    return HealthCheckResult(
        check_name="row_count",
        status=status,
        details=f"Row count: {count} (expected: [{min_rows}, {max_rows}])",
    )


@task
def check_value_in_range(data: list[dict[str, Any]], column: str, min_val: float, max_val: float) -> HealthCheckResult:
    """Check that all values in a column are within range.

    Args:
        data: Dataset rows.
        column: Column to check.
        min_val: Minimum acceptable value.
        max_val: Maximum acceptable value.

    Returns:
        HealthCheckResult.
    """
    values = [row[column] for row in data if column in row and isinstance(row[column], (int, float))]
    if not values:
        return HealthCheckResult(
            check_name=f"value_range:{column}",
            status="degraded",
            details=f"No numeric values found in column '{column}'",
        )
    out_of_range = [v for v in values if v < min_val or v > max_val]
    if not out_of_range:
        status = "healthy"
    elif len(out_of_range) / len(values) < 0.1:
        status = "degraded"
    else:
        status = "critical"
    return HealthCheckResult(
        check_name=f"value_range:{column}",
        status=status,
        details=f"{len(out_of_range)}/{len(values)} values out of [{min_val}, {max_val}]",
    )


@task
def run_health_check(check: HealthCheck, context: dict[str, Any]) -> HealthCheckResult:
    """Dispatch and run a health check.

    Args:
        check: Health check definition.
        context: Context dict with data/paths for checks.

    Returns:
        HealthCheckResult.
    """
    if check.check_type == "file_exists":
        return check_file_exists.fn(check.target)
    elif check.check_type == "file_freshness":
        return check_file_freshness.fn(check.target, check.params.get("max_age_seconds", 3600))
    elif check.check_type == "row_count":
        return check_row_count.fn(
            context.get("data", []),
            check.params.get("min_rows", 0),
            check.params.get("max_rows", 1000000),
        )
    elif check.check_type == "value_range":
        return check_value_in_range.fn(
            context.get("data", []),
            check.params.get("column", ""),
            check.params.get("min", 0),
            check.params.get("max", 1000),
        )
    return HealthCheckResult(
        check_name=check.name,
        status="critical",
        details=f"Unknown check type: {check.check_type}",
    )


@task
def aggregate_health(pipeline_name: str, results: list[HealthCheckResult]) -> PipelineHealthReport:
    """Aggregate health results using worst-status-wins.

    Args:
        pipeline_name: Name of the pipeline being monitored.
        results: Individual check results.

    Returns:
        Aggregated health report.
    """
    healthy = sum(1 for r in results if r.status == "healthy")
    degraded = sum(1 for r in results if r.status == "degraded")
    critical = sum(1 for r in results if r.status == "critical")

    if critical > 0:
        overall = "critical"
    elif degraded > 0:
        overall = "degraded"
    else:
        overall = "healthy"

    return PipelineHealthReport(
        pipeline_name=pipeline_name,
        overall_status=overall,
        healthy_count=healthy,
        degraded_count=degraded,
        critical_count=critical,
        results=results,
    )


@task
def publish_health_table(report: PipelineHealthReport) -> None:
    """Publish health results as a table artifact.

    Args:
        report: The health report.
    """
    table = [{"check": r.check_name, "status": r.status, "details": r.details} for r in report.results]
    create_table_artifact(
        key="pipeline-health",
        table=table,
        description=f"Health report for {report.pipeline_name}",
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="data_engineering_pipeline_health_monitor", log_prints=True)
def pipeline_health_monitor_flow(
    pipeline_name: str = "sample_pipeline",
    work_dir: str | None = None,
) -> PipelineHealthReport:
    """Monitor pipeline health by running defined checks.

    Args:
        pipeline_name: Name of the pipeline to monitor.
        work_dir: Optional working directory with pipeline outputs.

    Returns:
        Pipeline health report.
    """
    import tempfile

    if work_dir is None:
        work_dir = tempfile.mkdtemp(prefix="health_monitor_")

    # Create a sample output file
    base = Path(work_dir)
    base.mkdir(parents=True, exist_ok=True)
    output_file = base / "pipeline_output.csv"
    output_file.write_text("id,value\n1,100\n2,200\n3,300\n")

    sample_data = [
        {"id": 1, "value": 100},
        {"id": 2, "value": 200},
        {"id": 3, "value": 300},
    ]

    checks = [
        HealthCheck(name="output_exists", check_type="file_exists", target=str(output_file)),
        HealthCheck(
            name="output_fresh", check_type="file_freshness", target=str(output_file), params={"max_age_seconds": 3600}
        ),
        HealthCheck(name="row_count", check_type="row_count", params={"min_rows": 1, "max_rows": 100}),
        HealthCheck(name="value_range", check_type="value_range", params={"column": "value", "min": 0, "max": 500}),
    ]

    context = {"data": sample_data}
    results = [run_health_check(check, context) for check in checks]
    report = aggregate_health(pipeline_name, results)
    publish_health_table(report)

    print(f"Pipeline '{pipeline_name}' health: {report.overall_status}")
    return report


if __name__ == "__main__":
    pipeline_health_monitor_flow()
