"""Grand Capstone.

End-to-end analytics pipeline combining patterns from all 5 phases:
file I/O (csv), data quality rules, statistical profiling, regression
analysis, dimensional modeling, lineage tracking, and a final markdown
dashboard.

Airflow equivalent: None (combines patterns from all 5 phases).
Prefect approach:    Ingest CSV -> profile -> quality check -> enrich + dedup
                     -> regression -> dimensional model -> lineage -> dashboard.
"""

import csv
import hashlib
import math
import statistics
import tempfile
import time
from pathlib import Path

from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class CapstoneRecord(BaseModel):
    """A single capstone data record."""

    id: int
    name: str
    category: str
    value: float
    score: float
    spending: float
    value_tier: str = ""


class ProfileSummary(BaseModel):
    """Profile statistics for a dataset."""

    row_count: int
    column_count: int
    completeness: float


class CapstoneStage(BaseModel):
    """Metadata for a pipeline stage."""

    name: str
    status: str
    records_in: int
    records_out: int
    duration: float


class QualityResult(BaseModel):
    """Quality check result."""

    score: float
    rules_passed: int
    rules_total: int


class RegressionResult(BaseModel):
    """Regression analysis result."""

    slope: float
    intercept: float
    r_squared: float


class DimensionSummary(BaseModel):
    """Summary of dimensional model."""

    dimension_count: int
    fact_count: int
    composite_top: list[str]


class LineageEntry(BaseModel):
    """Single lineage tracking entry."""

    stage: str
    input_hash: str
    output_hash: str
    record_count: int


class CapstoneResult(BaseModel):
    """Grand capstone result combining all patterns."""

    stages: list[CapstoneStage]
    quality: QualityResult
    regression: RegressionResult
    dimensions: DimensionSummary
    lineage_entries: list[LineageEntry]
    total_duration: float


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def ingest_data(path: Path) -> list[CapstoneRecord]:
    """Ingest records from a CSV file.

    Args:
        path: Path to CSV file.

    Returns:
        List of CapstoneRecord.
    """
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    records: list[CapstoneRecord] = []
    for r in rows:
        records.append(
            CapstoneRecord(
                id=int(r.get("id", 0)),
                name=r.get("name", ""),
                category=r.get("category", ""),
                value=float(r.get("value", 0)),
                score=float(r.get("score", 0)),
                spending=float(r.get("spending", 0)),
            )
        )
    print(f"Ingested {len(records)} records")
    return records


@task
def profile_data(records: list[CapstoneRecord]) -> ProfileSummary:
    """Profile the dataset with basic statistics.

    Args:
        records: Input records.

    Returns:
        ProfileSummary.
    """
    if not records:
        return ProfileSummary(row_count=0, column_count=0, completeness=0.0)

    field_count = 6  # id, name, category, value, score, spending
    total_cells = len(records) * field_count
    null_cells = sum(1 for r in records for val in [r.name, r.category] if val == "")
    completeness = 1.0 - (null_cells / total_cells) if total_cells > 0 else 0.0

    return ProfileSummary(
        row_count=len(records),
        column_count=field_count,
        completeness=round(completeness, 4),
    )


@task
def run_quality_checks(records: list[CapstoneRecord]) -> QualityResult:
    """Run quality checks on the data.

    Args:
        records: Input records.

    Returns:
        QualityResult.
    """
    if not records:
        return QualityResult(score=0.0, rules_passed=0, rules_total=0)

    rules_passed = 0
    rules_total = 3

    # Rule 1: no zero IDs
    null_ids = sum(1 for r in records if r.id == 0)
    if null_ids == 0:
        rules_passed += 1

    # Rule 2: values in range
    bad_values = sum(1 for r in records if r.value < 0 or r.value > 10000)
    if bad_values == 0:
        rules_passed += 1

    # Rule 3: unique IDs
    ids = [r.id for r in records]
    if len(ids) == len(set(ids)):
        rules_passed += 1

    score = rules_passed / rules_total
    return QualityResult(score=round(score, 4), rules_passed=rules_passed, rules_total=rules_total)


@task
def enrich_and_deduplicate(records: list[CapstoneRecord]) -> list[CapstoneRecord]:
    """Enrich records with derived fields and deduplicate.

    Args:
        records: Input records.

    Returns:
        Enriched and deduplicated records.
    """
    # Enrich
    enriched: list[CapstoneRecord] = []
    for r in records:
        tier = "high" if r.value > 100 else "low"
        enriched.append(r.model_copy(update={"value_tier": tier}))

    # Deduplicate by id
    seen: set[str] = set()
    unique: list[CapstoneRecord] = []
    for r in enriched:
        key = str(r.id)
        h = hashlib.sha256(key.encode()).hexdigest()[:16]
        if h not in seen:
            seen.add(h)
            unique.append(r)
    print(f"Enriched and deduped: {len(records)} -> {len(unique)}")
    return unique


@task
def run_regression(records: list[CapstoneRecord], x_field: str, y_field: str) -> RegressionResult:
    """Perform simple linear regression.

    Args:
        records: Input records.
        x_field: X field name.
        y_field: Y field name.

    Returns:
        RegressionResult.
    """
    pairs = [(getattr(r, x_field), getattr(r, y_field)) for r in records]
    if len(pairs) < 3:
        return RegressionResult(slope=0.0, intercept=0.0, r_squared=0.0)

    x = [p[0] for p in pairs]
    y = [p[1] for p in pairs]
    n = len(x)
    mean_x = statistics.mean(x)
    mean_y = statistics.mean(y)

    cov_xy = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y, strict=True)) / n
    var_x = sum((xi - mean_x) ** 2 for xi in x) / n

    slope = cov_xy / var_x if var_x > 0 else 0.0
    intercept = mean_y - slope * mean_x

    ss_res = sum((yi - (slope * xi + intercept)) ** 2 for xi, yi in zip(x, y, strict=True))
    ss_tot = sum((yi - mean_y) ** 2 for yi in y)
    r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

    return RegressionResult(
        slope=round(slope, 6),
        intercept=round(intercept, 6),
        r_squared=round(r_squared, 6),
    )


@task
def build_dimensions(records: list[CapstoneRecord]) -> DimensionSummary:
    """Build a mini star schema from records.

    Args:
        records: Input records.

    Returns:
        DimensionSummary.
    """
    # Category dimension
    categories = list({r.category for r in records})
    # Tier dimension
    tiers = list({r.value_tier or "unknown" for r in records})
    # Fact count
    fact_count = len(records)

    # Composite ranking by value
    sorted_records = sorted(records, key=lambda r: r.value, reverse=True)
    top = [str(r.id) for r in sorted_records[:3]]

    dim_count = len(categories) + len(tiers)
    return DimensionSummary(dimension_count=dim_count, fact_count=fact_count, composite_top=top)


@task
def track_lineage(stages: list[CapstoneStage]) -> list[LineageEntry]:
    """Generate lineage entries from stage metadata.

    Args:
        stages: Pipeline stages.

    Returns:
        List of LineageEntry.
    """
    entries: list[LineageEntry] = []
    for s in stages:
        input_hash = hashlib.sha256(f"{s.name}:in:{s.records_in}".encode()).hexdigest()[:16]
        output_hash = hashlib.sha256(f"{s.name}:out:{s.records_out}".encode()).hexdigest()[:16]
        entries.append(
            LineageEntry(stage=s.name, input_hash=input_hash, output_hash=output_hash, record_count=s.records_out)
        )
    return entries


@task
def build_capstone_dashboard(result: CapstoneResult) -> str:
    """Build a markdown dashboard artifact.

    Args:
        result: Capstone result.

    Returns:
        Markdown string.
    """
    lines = [
        "# Grand Capstone Dashboard",
        "",
        f"**Quality Score:** {result.quality.score:.1%}",
        f"**R-squared:** {result.regression.r_squared:.4f}",
        f"**Total Duration:** {result.total_duration:.2f}s",
        "",
        "## Stages",
        "",
        "| Stage | Status | In | Out | Duration |",
        "|---|---|---|---|---|",
    ]
    for s in result.stages:
        lines.append(f"| {s.name} | {s.status} | {s.records_in} | {s.records_out} | {s.duration:.4f}s |")
    lines.extend(
        [
            "",
            "## Dimensions",
            "",
            f"**Dimension count:** {result.dimensions.dimension_count}",
            f"**Fact count:** {result.dimensions.fact_count}",
            f"**Top entities:** {', '.join(result.dimensions.composite_top)}",
            "",
            "## Lineage",
            "",
            f"**Stages tracked:** {len(result.lineage_entries)}",
        ]
    )
    markdown = "\n".join(lines)
    create_markdown_artifact(key="grand-capstone", markdown=markdown, description="Grand Capstone Dashboard")
    return markdown


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="analytics_grand_capstone", log_prints=True)
def grand_capstone_flow(work_dir: str | None = None) -> CapstoneResult:
    """Run the grand capstone pipeline.

    Args:
        work_dir: Working directory. Uses temp dir if not provided.

    Returns:
        CapstoneResult.
    """
    if work_dir is None:
        work_dir = tempfile.mkdtemp(prefix="capstone_")

    base = Path(work_dir)
    base.mkdir(parents=True, exist_ok=True)

    input_path = base / "input.csv"
    if not input_path.exists():
        _generate_capstone_csv(input_path)

    pipeline_start = time.monotonic()
    stages: list[CapstoneStage] = []

    # Stage 1: Ingest
    t0 = time.monotonic()
    records = ingest_data(input_path)
    stages.append(
        CapstoneStage(
            name="ingest",
            status="completed",
            records_in=0,
            records_out=len(records),
            duration=round(time.monotonic() - t0, 4),
        )
    )

    # Stage 2: Profile
    t0 = time.monotonic()
    profile_data(records)
    stages.append(
        CapstoneStage(
            name="profile",
            status="completed",
            records_in=len(records),
            records_out=len(records),
            duration=round(time.monotonic() - t0, 4),
        )
    )

    # Stage 3: Quality
    t0 = time.monotonic()
    quality = run_quality_checks(records)
    stages.append(
        CapstoneStage(
            name="quality",
            status="completed",
            records_in=len(records),
            records_out=len(records),
            duration=round(time.monotonic() - t0, 4),
        )
    )

    # Stage 4: Enrich + Dedup
    t0 = time.monotonic()
    cleaned = enrich_and_deduplicate(records)
    stages.append(
        CapstoneStage(
            name="enrich_dedup",
            status="completed",
            records_in=len(records),
            records_out=len(cleaned),
            duration=round(time.monotonic() - t0, 4),
        )
    )

    # Stage 5: Regression
    t0 = time.monotonic()
    regression = run_regression(cleaned, "value", "score")
    stages.append(
        CapstoneStage(
            name="regression",
            status="completed",
            records_in=len(cleaned),
            records_out=len(cleaned),
            duration=round(time.monotonic() - t0, 4),
        )
    )

    # Stage 6: Dimensions
    t0 = time.monotonic()
    dimensions = build_dimensions(cleaned)
    stages.append(
        CapstoneStage(
            name="dimensions",
            status="completed",
            records_in=len(cleaned),
            records_out=dimensions.fact_count,
            duration=round(time.monotonic() - t0, 4),
        )
    )

    # Stage 7: Lineage
    t0 = time.monotonic()
    lineage = track_lineage(stages)
    stages.append(
        CapstoneStage(
            name="lineage",
            status="completed",
            records_in=len(stages),
            records_out=len(lineage),
            duration=round(time.monotonic() - t0, 4),
        )
    )

    total_duration = round(time.monotonic() - pipeline_start, 4)

    result = CapstoneResult(
        stages=stages,
        quality=quality,
        regression=regression,
        dimensions=dimensions,
        lineage_entries=lineage,
        total_duration=total_duration,
    )
    build_capstone_dashboard(result)
    print(f"Grand capstone complete: quality={quality.score:.1%}, R2={regression.r_squared:.4f}")
    return result


def _generate_capstone_csv(path: Path) -> None:
    """Generate a sample CSV for the capstone pipeline."""
    fieldnames = ["id", "name", "category", "value", "score", "spending"]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(1, 31):
            writer.writerow(
                {
                    "id": i,
                    "name": f"entity_{i}",
                    "category": ["health", "education", "infrastructure"][i % 3],
                    "value": round(10.0 + i * 7.5, 1),
                    "score": round(50 + (i * 3.14) % 50, 1),
                    "spending": round(100 + i * 50 + math.sin(i) * 20, 1),
                }
            )
        # Add a duplicate
        writer.writerow(
            {"id": 1, "name": "entity_1", "category": "health", "value": 17.5, "score": 53.14, "spending": 150.0}
        )


if __name__ == "__main__":
    load_dotenv()
    grand_capstone_flow()
