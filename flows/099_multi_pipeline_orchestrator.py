"""099 -- Multi-Pipeline Orchestrator.

Orchestrate multiple independent mini-pipelines, collect status from
each, and produce a unified status rollup with overall health.

Airflow equivalent: None (Prefect-native pattern for orchestrating
                    independent pipelines).
Prefect approach:    Generate data, run 4 independent pipelines, aggregate
                     status, report with markdown artifact.
"""

import time

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class PipelineStatus(BaseModel):
    """Status of a single pipeline run."""

    pipeline_name: str
    status: str  # "success" or "failed"
    records_processed: int
    duration_seconds: float
    error: str


class PipelineDataRecord(BaseModel):
    """A single data record for pipeline processing."""

    id: int
    value: float
    category: str


class OrchestratorResult(BaseModel):
    """Aggregated result from all pipelines."""

    pipelines: list[PipelineStatus]
    successful: int
    failed: int
    total_records: int
    overall_status: str  # "healthy", "degraded", "critical"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def generate_pipeline_data() -> list[PipelineDataRecord]:
    """Generate sample data for the pipelines.

    Returns:
        List of PipelineDataRecord.
    """
    return [PipelineDataRecord(id=i, value=i * 10.0, category=["A", "B", "C"][i % 3]) for i in range(1, 51)]


@task
def run_ingest_pipeline(data: list[PipelineDataRecord]) -> PipelineStatus:
    """Run the ingest mini-pipeline.

    Args:
        data: Input data.

    Returns:
        PipelineStatus.
    """
    start = time.monotonic()
    count = len(data)
    duration = time.monotonic() - start
    return PipelineStatus(
        pipeline_name="ingest",
        status="success",
        records_processed=count,
        duration_seconds=round(duration, 4),
        error="",
    )


@task
def run_transform_pipeline(data: list[PipelineDataRecord]) -> PipelineStatus:
    """Run the transform mini-pipeline.

    Args:
        data: Input data.

    Returns:
        PipelineStatus.
    """
    start = time.monotonic()
    # Simulate transformation
    transformed = [r for r in data if r.value > 0]
    duration = time.monotonic() - start
    return PipelineStatus(
        pipeline_name="transform",
        status="success",
        records_processed=len(transformed),
        duration_seconds=round(duration, 4),
        error="",
    )


@task
def run_export_pipeline(data: list[PipelineDataRecord]) -> PipelineStatus:
    """Run the export mini-pipeline.

    Args:
        data: Input data.

    Returns:
        PipelineStatus.
    """
    start = time.monotonic()
    duration = time.monotonic() - start
    return PipelineStatus(
        pipeline_name="export",
        status="success",
        records_processed=len(data),
        duration_seconds=round(duration, 4),
        error="",
    )


@task
def run_quality_pipeline(data: list[PipelineDataRecord]) -> PipelineStatus:
    """Run the quality check mini-pipeline.

    Args:
        data: Input data.

    Returns:
        PipelineStatus.
    """
    start = time.monotonic()
    # Simulate quality check
    valid = sum(1 for r in data if r.value > 0)
    duration = time.monotonic() - start
    return PipelineStatus(
        pipeline_name="quality",
        status="success",
        records_processed=valid,
        duration_seconds=round(duration, 4),
        error="",
    )


@task
def aggregate_status(statuses: list[PipelineStatus]) -> OrchestratorResult:
    """Aggregate status from all pipelines.

    Overall health:
    - "healthy" if all pipelines succeed
    - "degraded" if any fail but majority succeed
    - "critical" if majority fail

    Args:
        statuses: Pipeline status results.

    Returns:
        OrchestratorResult.
    """
    successful = sum(1 for s in statuses if s.status == "success")
    failed = len(statuses) - successful
    total_records = sum(s.records_processed for s in statuses)

    if failed == 0:
        overall = "healthy"
    elif failed < len(statuses) / 2:
        overall = "degraded"
    else:
        overall = "critical"

    return OrchestratorResult(
        pipelines=statuses,
        successful=successful,
        failed=failed,
        total_records=total_records,
        overall_status=overall,
    )


@task
def orchestrator_report(result: OrchestratorResult) -> str:
    """Build a markdown artifact for the orchestrator result.

    Args:
        result: Orchestrator result.

    Returns:
        Markdown string.
    """
    lines = [
        "# Pipeline Orchestrator Report",
        "",
        f"**Overall Status:** {result.overall_status}",
        f"**Successful:** {result.successful} / {result.successful + result.failed}",
        f"**Total Records:** {result.total_records}",
        "",
        "| Pipeline | Status | Records | Duration |",
        "|---|---|---|---|",
    ]
    for p in result.pipelines:
        lines.append(f"| {p.pipeline_name} | {p.status} | {p.records_processed} | {p.duration_seconds:.4f}s |")
    markdown = "\n".join(lines)
    create_markdown_artifact(key="orchestrator-report", markdown=markdown, description="Orchestrator Report")
    return markdown


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="099_multi_pipeline_orchestrator", log_prints=True)
def multi_pipeline_orchestrator_flow() -> OrchestratorResult:
    """Run the multi-pipeline orchestrator.

    Returns:
        OrchestratorResult.
    """
    data = generate_pipeline_data()

    # Run 4 independent pipelines
    s1 = run_ingest_pipeline(data)
    s2 = run_transform_pipeline(data)
    s3 = run_export_pipeline(data)
    s4 = run_quality_pipeline(data)

    result = aggregate_status([s1, s2, s3, s4])
    orchestrator_report(result)
    print(f"Orchestrator: {result.overall_status} ({result.successful}/{result.successful + result.failed})")
    return result


if __name__ == "__main__":
    multi_pipeline_orchestrator_flow()
