"""108 -- DHIS2 Full Pipeline.

End-to-end DHIS2 pipeline with block config, real API calls, quality
checks, timing, and markdown dashboard.

Airflow equivalent: None (capstone combining all DHIS2 patterns).
Prefect approach:    Multi-stage pipeline with quality scoring and dashboard.
"""

from __future__ import annotations

import time
from typing import Any

import httpx
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
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


class PipelineStage(BaseModel):
    """Result of a single pipeline stage."""

    name: str
    status: str
    record_count: int
    duration: float


class QualityResult(BaseModel):
    """Quality validation result."""

    checks_passed: int
    checks_total: int
    score: float
    issues: list[str]


class Dhis2PipelineResult(BaseModel):
    """Full pipeline result."""

    stages: list[PipelineStage]
    total_records: int
    quality_score: float
    duration: float


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def connect_and_verify(conn: Dhis2Connection, password: str) -> Dhis2ApiResponse:
    """Connect to DHIS2 and verify access via system/info.

    Args:
        conn: DHIS2 connection block.
        password: DHIS2 password.

    Returns:
        Dhis2ApiResponse from verification.
    """
    url = f"{conn.base_url}/api/system/info"
    resp = httpx.get(url, auth=(conn.username, password), timeout=30)
    resp.raise_for_status()
    data: dict[str, Any] = resp.json()
    print(f"Connected to {conn.base_url}, DHIS2 v{data.get('version', 'unknown')}")
    return Dhis2ApiResponse(endpoint="system/info", record_count=1, status_code=resp.status_code)


@task
def fetch_all_metadata(
    conn: Dhis2Connection,
    password: str,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch org units, data elements, and indicators from the DHIS2 API.

    Args:
        conn: DHIS2 connection block.
        password: DHIS2 password.

    Returns:
        Dict mapping endpoint name to list of records.
    """
    org_units = fetch_metadata(conn, "organisationUnits", password)
    data_elements = fetch_metadata(conn, "dataElements", password)
    indicators = fetch_metadata(conn, "indicators", password)
    result: dict[str, list[dict[str, Any]]] = {
        "organisationUnits": org_units,
        "dataElements": data_elements,
        "indicators": indicators,
    }
    total = sum(len(v) for v in result.values())
    print(f"Fetched metadata: {total} total records across {len(result)} endpoints")
    return result


@task
def validate_metadata(metadata: dict[str, list[dict[str, Any]]]) -> QualityResult:
    """Run quality checks on fetched metadata.

    Checks:
    - Each endpoint has records (non-empty)
    - Org units have valid levels (> 0)
    - Data elements have required fields (id, name, valueType)
    - Indicators have expressions (numerator, denominator)

    Args:
        metadata: Fetched metadata by endpoint.

    Returns:
        QualityResult.
    """
    checks_total = 0
    checks_passed = 0
    issues: list[str] = []

    # Check non-empty endpoints
    for endpoint, records in metadata.items():
        checks_total += 1
        if records:
            checks_passed += 1
        else:
            issues.append(f"{endpoint}: no records")

    # Check org unit levels
    for ou in metadata.get("organisationUnits", []):
        checks_total += 1
        level = ou.get("level", 0)
        if isinstance(level, int) and level > 0:
            checks_passed += 1
        else:
            issues.append(f"OrgUnit {ou.get('id', '?')}: invalid level {level}")

    # Check data element required fields
    for de in metadata.get("dataElements", []):
        checks_total += 1
        if all(de.get(f) for f in ["id", "name", "valueType"]):
            checks_passed += 1
        else:
            issues.append(f"DataElement {de.get('id', '?')}: missing required fields")

    # Check indicator expressions
    for ind in metadata.get("indicators", []):
        checks_total += 1
        if ind.get("numerator") and ind.get("denominator"):
            checks_passed += 1
        else:
            issues.append(f"Indicator {ind.get('id', '?')}: missing expression")

    score = checks_passed / checks_total if checks_total > 0 else 0.0
    print(f"Quality: {checks_passed}/{checks_total} checks passed ({score:.1%})")
    return QualityResult(
        checks_passed=checks_passed,
        checks_total=checks_total,
        score=round(score, 4),
        issues=issues,
    )


@task
def build_dashboard(result: Dhis2PipelineResult) -> str:
    """Build a markdown dashboard summarizing the pipeline run.

    Args:
        result: Pipeline result.

    Returns:
        Markdown string.
    """
    lines = [
        "## DHIS2 Pipeline Dashboard",
        "",
        f"- Total records: {result.total_records}",
        f"- Quality score: {result.quality_score:.1%}",
        f"- Duration: {result.duration:.2f}s",
        "",
        "### Stages",
        "",
        "| Stage | Status | Records | Duration |",
        "|-------|--------|---------|----------|",
    ]
    for s in result.stages:
        lines.append(f"| {s.name} | {s.status} | {s.record_count} | {s.duration:.3f}s |")
    md = "\n".join(lines)
    print("Dashboard built")
    return md


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="108_dhis2_pipeline", log_prints=True)
def dhis2_pipeline_flow() -> Dhis2PipelineResult:
    """Run an end-to-end DHIS2 metadata pipeline.

    Returns:
        Dhis2PipelineResult.
    """
    pipeline_start = time.monotonic()
    stages: list[PipelineStage] = []

    conn = get_dhis2_connection()
    password = get_dhis2_password()

    # Stage 1: Connect and verify
    t0 = time.monotonic()
    connect_and_verify(conn, password)
    stages.append(PipelineStage(name="connect", status="completed", record_count=1, duration=time.monotonic() - t0))

    # Stage 2: Fetch all metadata
    t0 = time.monotonic()
    metadata = fetch_all_metadata(conn, password)
    total_records = sum(len(v) for v in metadata.values())
    stages.append(
        PipelineStage(name="fetch", status="completed", record_count=total_records, duration=time.monotonic() - t0)
    )

    # Stage 3: Validate quality
    t0 = time.monotonic()
    quality = validate_metadata(metadata)
    stages.append(
        PipelineStage(
            name="validate", status="completed", record_count=quality.checks_total, duration=time.monotonic() - t0
        )
    )

    pipeline_duration = time.monotonic() - pipeline_start

    result = Dhis2PipelineResult(
        stages=stages,
        total_records=total_records,
        quality_score=quality.score,
        duration=round(pipeline_duration, 4),
    )

    dashboard_md = build_dashboard(result)
    create_markdown_artifact(key="dhis2-pipeline-dashboard", markdown=dashboard_md)

    print(f"Pipeline complete: {result.total_records} records, quality {result.quality_score:.1%}")
    return result


if __name__ == "__main__":
    dhis2_pipeline_flow()
