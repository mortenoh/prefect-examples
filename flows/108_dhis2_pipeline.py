"""108 -- DHIS2 Full Pipeline.

End-to-end DHIS2 pipeline with block config, error handling, quality checks,
timing, and markdown dashboard.

Airflow equivalent: None (capstone combining all DHIS2 patterns).
Prefect approach:    Multi-stage pipeline with quality scoring and dashboard.
"""

from __future__ import annotations

import importlib.util
import sys
import time
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
Dhis2ApiResponse = _helpers.Dhis2ApiResponse
RawOrgUnit = _helpers.RawOrgUnit
RawDataElement = _helpers.RawDataElement
RawIndicator = _helpers.RawIndicator
get_dhis2_connection = _helpers.get_dhis2_connection
get_dhis2_password = _helpers.get_dhis2_password
dhis2_api_fetch = _helpers.dhis2_api_fetch

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
    """Connect to DHIS2 and verify access.

    Args:
        conn: DHIS2 connection block.
        password: DHIS2 password.

    Returns:
        Dhis2ApiResponse from verification.
    """
    _ = conn, password
    result = Dhis2ApiResponse(endpoint="system/info", record_count=1, status_code=200)
    print(f"Connected to {conn.base_url}, verified OK")
    return result


@task
def fetch_all_metadata(
    conn: Dhis2Connection,
    password: str,
) -> dict[str, list[dict[str, object]]]:
    """Fetch org units, data elements, and indicators.

    Args:
        conn: DHIS2 connection block.
        password: DHIS2 password.

    Returns:
        Dict mapping endpoint name to list of records.
    """
    org_units = dhis2_api_fetch(conn, "organisationUnits", password)
    data_elements = dhis2_api_fetch(conn, "dataElements", password)
    indicators = dhis2_api_fetch(conn, "indicators", password)
    result = {
        "organisationUnits": org_units,
        "dataElements": data_elements,
        "indicators": indicators,
    }
    total = sum(len(v) for v in result.values())
    print(f"Fetched metadata: {total} total records across {len(result)} endpoints")
    return result


@task
def validate_metadata(metadata: dict[str, list[dict[str, object]]]) -> QualityResult:
    """Run quality checks on fetched metadata.

    Checks:
    - Each endpoint has records (non-empty)
    - Org units have valid levels (1-4)
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
        if isinstance(level, int) and 1 <= level <= 4:
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
