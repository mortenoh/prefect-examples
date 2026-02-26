"""Complex Pipeline.

End-to-end pipeline combining subflows, mapped tasks, and notifications.

Airflow equivalent: complex DAG with branching, sensors, callbacks.
Prefect approach:    combine subflows, .map(), result passing, hooks.
"""

from __future__ import annotations

import datetime

from dotenv import load_dotenv
from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class PipelineRecord(BaseModel):
    """A single record flowing through the pipeline."""

    id: int
    name: str
    score: int
    valid: bool = False
    enriched: bool = False
    processed_at: str = ""


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def validate_record(record: PipelineRecord) -> PipelineRecord:
    """Mark a record as valid after basic checks.

    Args:
        record: A single data record.

    Returns:
        The record with the valid flag set.
    """
    validated = record.model_copy(update={"valid": True})
    print(f"Validated record {record.id}")
    return validated


@task
def enrich_record(record: PipelineRecord) -> PipelineRecord:
    """Add enrichment metadata to a validated record.

    Args:
        record: A validated data record.

    Returns:
        The record with enriched flag and processed_at timestamp.
    """
    enriched = record.model_copy(
        update={
            "enriched": True,
            "processed_at": datetime.datetime.now(datetime.UTC).isoformat(),
        }
    )
    print(f"Enriched record {record.id}")
    return enriched


@task
def notify(summary: str) -> None:
    """Send a notification with the pipeline summary.

    Args:
        summary: The summary string to include in the notification.
    """
    print(f"NOTIFICATION: {summary}")


# ---------------------------------------------------------------------------
# Subflows
# ---------------------------------------------------------------------------


@flow(name="basics_extract", log_prints=True)
def extract_stage() -> list[PipelineRecord]:
    """Extract sample records from the source system.

    Returns:
        A list of raw PipelineRecord objects.
    """
    records = [
        PipelineRecord(id=1, name="Alice", score=88),
        PipelineRecord(id=2, name="Bob", score=95),
        PipelineRecord(id=3, name="Charlie", score=72),
        PipelineRecord(id=4, name="Diana", score=64),
    ]
    print(f"Extracted {len(records)} records")
    return records


@flow(name="basics_transform", log_prints=True)
def transform_stage(raw: list[PipelineRecord]) -> list[PipelineRecord]:
    """Validate and enrich raw records using mapped tasks.

    Args:
        raw: List of raw PipelineRecord objects.

    Returns:
        List of validated and enriched records.
    """
    validated = validate_record.map(raw)
    enriched = enrich_record.map(validated)
    results = [future.result() for future in enriched]
    print(f"Transformed {len(results)} records")
    return results


@flow(name="basics_load", log_prints=True)
def load_stage(data: list[PipelineRecord]) -> str:
    """Persist processed records and return a summary.

    Args:
        data: List of enriched PipelineRecord objects.

    Returns:
        A summary string describing the loaded data.
    """
    valid_count = sum(1 for r in data if r.valid)
    enriched_count = sum(1 for r in data if r.enriched)
    summary = f"Loaded {len(data)} records ({valid_count} valid, {enriched_count} enriched)"
    print(summary)
    return summary


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


@flow(name="basics_complex_pipeline", log_prints=True)
def complex_pipeline() -> None:
    """Run the full extract -> transform -> load -> notify pipeline.

    This is the capstone flow for Phase 1, demonstrating subflows,
    mapped tasks, result passing, and post-pipeline notifications.
    """
    raw = extract_stage()
    transformed = transform_stage(raw)
    summary = load_stage(transformed)
    notify(summary)
    print("Pipeline complete")


if __name__ == "__main__":
    load_dotenv()
    complex_pipeline()
