"""020 — Complex Pipeline.

End-to-end pipeline combining subflows, mapped tasks, and notifications.

Airflow equivalent: complex DAG with branching, sensors, callbacks.
Prefect approach:    combine subflows, .map(), result passing, hooks.
"""

import datetime

from prefect import flow, task

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def validate_record(record: dict) -> dict:
    """Mark a record as valid after basic checks.

    Args:
        record: A single data record.

    Returns:
        The record with a "valid" flag added.
    """
    validated = {**record, "valid": True}
    print(f"Validated record {record.get('id')}")
    return validated


@task
def enrich_record(record: dict) -> dict:
    """Add enrichment metadata to a validated record.

    Args:
        record: A validated data record.

    Returns:
        The record with "enriched" flag and "processed_at" timestamp.
    """
    enriched = {
        **record,
        "enriched": True,
        "processed_at": datetime.datetime.now(datetime.UTC).isoformat(),
    }
    print(f"Enriched record {record.get('id')}")
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


@flow(name="020_extract", log_prints=True)
def extract_stage() -> list[dict]:
    """Extract sample records from the source system.

    Returns:
        A list of raw record dictionaries.
    """
    records = [
        {"id": 1, "name": "Alice", "score": 88},
        {"id": 2, "name": "Bob", "score": 95},
        {"id": 3, "name": "Charlie", "score": 72},
        {"id": 4, "name": "Diana", "score": 64},
    ]
    print(f"Extracted {len(records)} records")
    return records


@flow(name="020_transform", log_prints=True)
def transform_stage(raw: list[dict]) -> list[dict]:
    """Validate and enrich raw records using mapped tasks.

    Args:
        raw: List of raw record dictionaries.

    Returns:
        List of validated and enriched records.
    """
    validated = validate_record.map(raw)
    enriched = enrich_record.map(validated)
    results = [future.result() for future in enriched]
    print(f"Transformed {len(results)} records")
    return results


@flow(name="020_load", log_prints=True)
def load_stage(data: list[dict]) -> str:
    """Persist processed records and return a summary.

    Args:
        data: List of enriched record dictionaries.

    Returns:
        A summary string describing the loaded data.
    """
    valid_count = sum(1 for r in data if r.get("valid"))
    enriched_count = sum(1 for r in data if r.get("enriched"))
    summary = f"Loaded {len(data)} records ({valid_count} valid, {enriched_count} enriched)"
    print(summary)
    return summary


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


@flow(name="020_complex_pipeline", log_prints=True)
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
    complex_pipeline()
