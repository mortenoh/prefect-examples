"""Production Pipeline v3.

Phase 4 capstone combining file I/O, data profiling, quality rules,
enrichment with caching, deduplication, and checkpointing.

Airflow equivalent: Quality framework + dashboard capstone (DAGs 099, 098).
Prefect approach:    Compose all Phase 4 patterns into a realistic pipeline
                     with timing, checkpoints, and a markdown dashboard.
"""

import csv
import datetime
import hashlib
import statistics
import tempfile
import time
from pathlib import Path
from typing import Any

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class PipelineStage(BaseModel):
    """Metadata for a single pipeline stage."""

    name: str
    status: str
    records_in: int
    records_out: int
    duration_seconds: float


class PipelineV3Result(BaseModel):
    """Complete pipeline result."""

    stages: list[PipelineStage]
    quality_score: float
    traffic_light: str
    enrichment_rate: float
    dedup_rate: float
    profile_summary: dict[str, Any]
    total_duration_seconds: float


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def ingest_csv(path: Path) -> list[dict[str, Any]]:
    """Ingest records from a CSV file.

    Args:
        path: Path to the CSV file.

    Returns:
        List of record dicts.
    """
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        records = list(reader)
    # Convert numeric fields
    import contextlib

    for r in records:
        for key in ["value", "score"]:
            if key in r:
                with contextlib.suppress(ValueError, TypeError):
                    r[key] = float(r[key])
        if "id" in r:
            with contextlib.suppress(ValueError, TypeError):
                r["id"] = int(r["id"])
    print(f"Ingested {len(records)} records from {path.name}")
    return records


@task
def profile_data(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Profile the dataset with basic statistics.

    Args:
        records: Input records.

    Returns:
        Profile summary dict.
    """
    if not records:
        return {"row_count": 0, "columns": [], "completeness": 0.0}

    columns = list(records[0].keys())
    total_cells = len(records) * len(columns)
    null_cells = sum(1 for r in records for c in columns if r.get(c) is None or r.get(c) == "")
    completeness = 1.0 - (null_cells / total_cells) if total_cells > 0 else 0.0

    numeric_cols = {}
    for col in columns:
        values = [r[col] for r in records if isinstance(r.get(col), (int, float))]
        if len(values) >= 2:
            numeric_cols[col] = {
                "mean": round(statistics.mean(values), 2),
                "stdev": round(statistics.stdev(values), 2),
            }

    return {
        "row_count": len(records),
        "column_count": len(columns),
        "columns": columns,
        "completeness": round(completeness, 4),
        "numeric_profiles": numeric_cols,
    }


@task
def run_quality_checks(records: list[dict[str, Any]], rules: list[dict[str, Any]]) -> dict[str, Any]:
    """Run quality checks on the data.

    Args:
        records: Input records.
        rules: List of rule config dicts.

    Returns:
        Quality check results with score and traffic light.
    """
    results: list[dict[str, Any]] = []
    for rule in rules:
        col = rule.get("column", "")
        rule_type = rule["type"]
        if rule_type == "not_null":
            nulls = sum(1 for r in records if not r.get(col))
            score = 1.0 - (nulls / len(records)) if records else 0.0
            results.append({"rule": f"not_null:{col}", "score": score, "passed": nulls == 0})
        elif rule_type == "range":
            min_v, max_v = rule.get("min", 0), rule.get("max", 1000)
            bad = sum(1 for r in records if isinstance(r.get(col), (int, float)) and (r[col] < min_v or r[col] > max_v))
            score = 1.0 - (bad / len(records)) if records else 0.0
            results.append({"rule": f"range:{col}", "score": score, "passed": bad == 0})
        elif rule_type == "unique":
            vals = [r.get(col) for r in records]
            score = len(set(vals)) / len(vals) if vals else 1.0
            results.append({"rule": f"unique:{col}", "score": score, "passed": len(set(vals)) == len(vals)})

    overall: float = sum(float(r["score"]) for r in results) / len(results) if results else 0.0
    if overall >= 0.9:
        traffic_light = "green"
    elif overall >= 0.7:
        traffic_light = "amber"
    else:
        traffic_light = "red"

    return {"overall_score": round(overall, 4), "traffic_light": traffic_light, "rules": results}


@task
def enrich_records(records: list[dict[str, Any]], cache: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Enrich records with simulated external data, using a cache.

    Args:
        records: Input records.
        cache: Shared cache dict.

    Returns:
        Tuple of (enriched records, cache stats).
    """
    hits = 0
    misses = 0
    enriched = []
    for r in records:
        rid = r.get("id", "")
        cache_key = f"enrich:{rid}"
        if cache_key in cache:
            extra = cache[cache_key]
            hits += 1
        else:
            extra = {"tier": "premium" if hash(str(rid)) % 3 == 0 else "standard", "risk": "low"}
            cache[cache_key] = extra
            misses += 1
        enriched.append({**r, **extra})

    total = hits + misses
    return enriched, {"hits": hits, "misses": misses, "hit_rate": round(hits / total, 2) if total > 0 else 0.0}


@task
def deduplicate_records(records: list[dict[str, Any]], key_fields: list[str]) -> list[dict[str, Any]]:
    """Deduplicate records by hashing key fields.

    Args:
        records: Input records.
        key_fields: Fields to use for dedup hash.

    Returns:
        Deduplicated records.
    """
    seen: set[str] = set()
    unique = []
    for r in records:
        raw = "|".join(str(r.get(f, "")) for f in sorted(key_fields))
        h = hashlib.sha256(raw.encode()).hexdigest()[:16]
        if h not in seen:
            seen.add(h)
            unique.append(r)
    return unique


@task
def write_output(records: list[dict[str, Any]], path: Path) -> str:
    """Write output records to CSV.

    Args:
        records: Records to write.
        path: Output file path.

    Returns:
        Path string.
    """
    if not records:
        path.write_text("")
        return str(path)
    fieldnames = list(records[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    print(f"Wrote {len(records)} records to {path.name}")
    return str(path)


@task
def build_dashboard(result: PipelineV3Result) -> str:
    """Build a markdown dashboard artifact.

    Args:
        result: Pipeline result.

    Returns:
        Markdown string.
    """
    lines = [
        "# Production Pipeline v3 Dashboard",
        "",
        f"**Quality:** {result.traffic_light.upper()} ({result.quality_score:.1%})",
        f"**Enrichment Rate:** {result.enrichment_rate:.0%}",
        f"**Dedup Rate:** {result.dedup_rate:.1%}",
        f"**Duration:** {result.total_duration_seconds:.1f}s",
        "",
        "## Stages",
        "",
        "| Stage | Status | In | Out | Duration |",
        "|---|---|---|---|---|",
    ]
    for s in result.stages:
        lines.append(f"| {s.name} | {s.status} | {s.records_in} | {s.records_out} | {s.duration_seconds:.2f}s |")

    lines.extend(
        [
            "",
            "## Profile",
            "",
            f"**Rows:** {result.profile_summary.get('row_count', 0)}",
            f"**Completeness:** {result.profile_summary.get('completeness', 0):.1%}",
        ]
    )

    markdown = "\n".join(lines)
    create_markdown_artifact(key="pipeline-v3-dashboard", markdown=markdown, description="Pipeline v3 dashboard")
    return markdown


@task
def save_pipeline_checkpoint(stage: str, data: dict[str, Any], directory: str) -> None:
    """Save a checkpoint for the pipeline stage.

    Args:
        stage: Stage name.
        data: Stage data to checkpoint.
        directory: Checkpoint directory.
    """
    path = Path(directory) / f"checkpoint_{stage}.json"
    import json

    path.write_text(
        json.dumps(
            {
                "stage": stage,
                "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
                "summary": {"count": data.get("count", len(data))},
            }
        )
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="data_engineering_production_pipeline_v3", log_prints=True)
def production_pipeline_v3_flow(work_dir: str | None = None) -> PipelineV3Result:
    """Run the Phase 4 capstone pipeline.

    Args:
        work_dir: Working directory. Uses temp dir if not provided.

    Returns:
        PipelineV3Result.
    """
    if work_dir is None:
        work_dir = tempfile.mkdtemp(prefix="pipeline_v3_")

    base = Path(work_dir)
    base.mkdir(parents=True, exist_ok=True)
    cp_dir = str(base / "checkpoints")
    Path(cp_dir).mkdir(parents=True, exist_ok=True)

    # Generate input CSV
    input_path = base / "input.csv"
    if not input_path.exists():
        _generate_input(input_path)

    pipeline_start = time.monotonic()
    stages: list[PipelineStage] = []
    cache: dict[str, Any] = {}

    # Stage 1: Ingest
    t0 = time.monotonic()
    records = ingest_csv(input_path)
    stages.append(
        PipelineStage(
            name="ingest",
            status="completed",
            records_in=0,
            records_out=len(records),
            duration_seconds=round(time.monotonic() - t0, 4),
        )
    )
    save_pipeline_checkpoint("ingest", {"count": len(records)}, cp_dir)

    # Stage 2: Profile
    t0 = time.monotonic()
    profile = profile_data(records)
    stages.append(
        PipelineStage(
            name="profile",
            status="completed",
            records_in=len(records),
            records_out=len(records),
            duration_seconds=round(time.monotonic() - t0, 4),
        )
    )

    # Stage 3: Quality
    t0 = time.monotonic()
    rules: list[dict[str, Any]] = [
        {"type": "not_null", "column": "id"},
        {"type": "not_null", "column": "name"},
        {"type": "range", "column": "value", "min": 0, "max": 500},
        {"type": "unique", "column": "id"},
    ]
    quality = run_quality_checks(records, rules)
    stages.append(
        PipelineStage(
            name="quality",
            status="completed",
            records_in=len(records),
            records_out=len(records),
            duration_seconds=round(time.monotonic() - t0, 4),
        )
    )

    # Stage 4: Enrich
    t0 = time.monotonic()
    enriched, _cache_stats = enrich_records(records, cache)
    stages.append(
        PipelineStage(
            name="enrich",
            status="completed",
            records_in=len(records),
            records_out=len(enriched),
            duration_seconds=round(time.monotonic() - t0, 4),
        )
    )

    # Stage 5: Deduplicate
    t0 = time.monotonic()
    deduped = deduplicate_records(enriched, ["id", "name"])
    dedup_rate = 1.0 - (len(deduped) / len(enriched)) if enriched else 0.0
    stages.append(
        PipelineStage(
            name="dedup",
            status="completed",
            records_in=len(enriched),
            records_out=len(deduped),
            duration_seconds=round(time.monotonic() - t0, 4),
        )
    )

    # Stage 6: Write output
    t0 = time.monotonic()
    output_path = base / "output.csv"
    write_output(deduped, output_path)
    stages.append(
        PipelineStage(
            name="write",
            status="completed",
            records_in=len(deduped),
            records_out=len(deduped),
            duration_seconds=round(time.monotonic() - t0, 4),
        )
    )
    save_pipeline_checkpoint("write", {"count": len(deduped)}, cp_dir)

    total_duration = round(time.monotonic() - pipeline_start, 4)

    result = PipelineV3Result(
        stages=stages,
        quality_score=quality["overall_score"],
        traffic_light=quality["traffic_light"],
        enrichment_rate=1.0,
        dedup_rate=round(dedup_rate, 4),
        profile_summary=profile,
        total_duration_seconds=total_duration,
    )

    build_dashboard(result)
    print(f"Pipeline v3 complete: {quality['traffic_light']} quality, {len(deduped)} output records")
    return result


def _generate_input(path: Path) -> None:
    """Generate a sample input CSV for the pipeline."""
    fieldnames = ["id", "name", "value", "score", "category"]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(1, 26):
            writer.writerow(
                {
                    "id": i,
                    "name": f"entity_{i}" if i % 10 != 0 else "",
                    "value": round(10.0 + i * 7.3, 1),
                    "score": round(50 + (i * 3.14) % 50, 1),
                    "category": ["alpha", "beta", "gamma"][i % 3],
                }
            )
        # Add a duplicate
        writer.writerow({"id": 1, "name": "entity_1", "value": 17.3, "score": 53.14, "category": "beta"})


if __name__ == "__main__":
    production_pipeline_v3_flow()
