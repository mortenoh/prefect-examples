"""Production Pipeline.

Capstone flow for Phase 2: combines caching, retries, artifacts, tags,
result persistence, and structured logging into a production-ready pipeline.

Airflow equivalent: Production DAG with sensors, retries, SLAs, callbacks.
Prefect approach:    Compose Phase 2 features into a realistic pipeline.
"""

import datetime

from prefect import flow, get_run_logger, tags, task
from prefect.artifacts import create_markdown_artifact
from prefect.cache_policies import INPUTS

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=1)
def validate(record: dict) -> dict:
    """Validate a record with automatic retries.

    Args:
        record: A raw data record.

    Returns:
        The record with a "valid" flag.
    """
    logger = get_run_logger()
    logger.info("Validating record %s", record.get("id"))
    return {**record, "valid": True}


@task(cache_policy=INPUTS)
def enrich(record: dict) -> dict:
    """Enrich a record with metadata, cached by input.

    Args:
        record: A validated data record.

    Returns:
        The record with enrichment metadata.
    """
    enriched = {
        **record,
        "enriched": True,
        "enriched_at": datetime.datetime.now(datetime.UTC).isoformat(),
    }
    print(f"Enriched record {record.get('id')}")
    return enriched


@task
def notify(summary: str) -> None:
    """Publish a pipeline summary as a markdown artifact.

    Args:
        summary: The pipeline summary string.
    """
    markdown = f"""# Pipeline Summary

**Completed at:** {datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M UTC")}

{summary}
"""
    create_markdown_artifact(
        key="production-pipeline-summary",
        markdown=markdown,
        description="Production pipeline run summary",
    )
    print(f"NOTIFICATION: {summary}")


# ---------------------------------------------------------------------------
# Subflows
# ---------------------------------------------------------------------------


@flow(name="core_extract", log_prints=True)
def extract_stage() -> list[dict]:
    """Extract records from the source system.

    Returns:
        A list of raw record dicts.
    """
    records = [
        {"id": 1, "name": "Alice", "value": 100},
        {"id": 2, "name": "Bob", "value": 250},
        {"id": 3, "name": "Charlie", "value": 175},
        {"id": 4, "name": "Diana", "value": 300},
        {"id": 5, "name": "Eve", "value": 125},
    ]
    print(f"Extracted {len(records)} records")
    return records


@flow(name="core_transform", log_prints=True)
def transform_stage(records: list[dict]) -> list[dict]:
    """Validate and enrich records using Phase 2 features.

    Uses retries on validate, caching on enrich, and mapped execution.

    Args:
        records: List of raw record dicts.

    Returns:
        List of validated and enriched records.
    """
    validated = validate.map(records)
    enriched = enrich.map(validated)
    results = [future.result() for future in enriched]
    print(f"Transformed {len(results)} records")
    return results


@flow(name="core_load", log_prints=True, persist_result=True)
def load_stage(records: list[dict]) -> str:
    """Load processed records and return a summary.

    Args:
        records: List of enriched record dicts.

    Returns:
        A summary string.
    """
    valid_count = sum(1 for r in records if r.get("valid"))
    enriched_count = sum(1 for r in records if r.get("enriched"))
    total_value = sum(r.get("value", 0) for r in records)
    summary = (
        f"Loaded {len(records)} records: {valid_count} valid, {enriched_count} enriched, total value={total_value}"
    )
    print(summary)
    return summary


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


@flow(name="core_production_pipeline", log_prints=True)
def production_pipeline() -> None:
    """Run the full production pipeline combining Phase 2 concepts.

    Features used:
    - Task caching (INPUTS policy on enrich)
    - Retries (on validate)
    - Artifacts (markdown summary via notify)
    - Tags (via context manager)
    - Result persistence (on load_stage)
    - Structured logging (get_run_logger in validate)
    """
    logger = get_run_logger()
    logger.info("Starting production pipeline")

    with tags("production", "phase-2"):
        raw = extract_stage()
        transformed = transform_stage(raw)
        summary = load_stage(transformed)
        notify(summary)

    logger.info("Production pipeline complete")


if __name__ == "__main__":
    production_pipeline()
