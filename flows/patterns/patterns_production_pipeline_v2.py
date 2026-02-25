"""Production Pipeline v2.

Capstone flow for Phase 3: combines Pydantic models, transactions, retries,
artifacts, runtime context, state hooks, and .map() into a production pipeline.

Airflow equivalent: Full ETL SCD capstone (DAG 100).
Prefect approach:    Compose all Phase 3 features into a realistic pipeline.
"""

import datetime
from typing import Any

from prefect import flow, get_run_logger, tags, task
from prefect.artifacts import create_markdown_artifact
from prefect.transactions import transaction
from pydantic import BaseModel, field_validator

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class SourceRecord(BaseModel):
    """A raw record from the source system."""

    id: int
    name: str
    value: float
    region: str


class ValidatedRecord(BaseModel):
    """A record that has passed validation."""

    id: int
    name: str
    value: float
    region: str
    valid: bool = True

    @field_validator("value")
    @classmethod
    def value_must_be_positive(cls, v: float) -> float:
        """Ensure value is non-negative."""
        if v < 0:
            raise ValueError(f"Value must be non-negative, got {v}")
        return v


class PipelineMetrics(BaseModel):
    """Metrics collected during pipeline execution."""

    total_records: int
    valid_records: int
    invalid_records: int
    total_value: float
    regions: list[str]


# ---------------------------------------------------------------------------
# Hooks
# ---------------------------------------------------------------------------


def on_pipeline_completion(flow: Any, flow_run: Any, state: Any) -> None:
    """Log pipeline completion.

    Args:
        flow: The flow object.
        flow_run: The flow-run metadata.
        state: The final state.
    """
    print(f"HOOK  Pipeline {flow_run.name!r} completed: {state.name}")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=1)
def extract_record(record_data: dict[str, Any]) -> SourceRecord:
    """Extract and parse a single source record.

    Args:
        record_data: Raw record dict from the source.

    Returns:
        A validated SourceRecord.
    """
    record = SourceRecord(**record_data)
    print(f"Extracted record {record.id}: {record.name}")
    return record


@task
def validate_record(record: SourceRecord) -> ValidatedRecord | None:
    """Validate a source record using Pydantic.

    Args:
        record: The source record to validate.

    Returns:
        A ValidatedRecord if valid, None if invalid.
    """
    try:
        validated = ValidatedRecord(**record.model_dump())
        return validated
    except Exception as e:
        print(f"Validation failed for record {record.id}: {e}")
        return None


@task
def transform_record(record: ValidatedRecord) -> dict[str, Any]:
    """Apply business transformations to a validated record.

    Args:
        record: A validated record.

    Returns:
        A dict with the transformed data.
    """
    transformed = record.model_dump()
    transformed["processed_at"] = datetime.datetime.now(datetime.UTC).isoformat()
    transformed["value_category"] = "high" if record.value >= 200 else "standard"
    return transformed


@task
def compute_metrics(records: list[dict[str, Any]]) -> PipelineMetrics:
    """Compute pipeline metrics from processed records.

    Args:
        records: List of processed record dicts.

    Returns:
        Aggregated pipeline metrics.
    """
    valid = [r for r in records if r.get("valid")]
    metrics = PipelineMetrics(
        total_records=len(records),
        valid_records=len(valid),
        invalid_records=len(records) - len(valid),
        total_value=sum(r.get("value", 0) for r in valid),
        regions=sorted(set(r.get("region", "unknown") for r in valid)),
    )
    print(f"Metrics: {metrics.valid_records}/{metrics.total_records} valid")
    return metrics


@task
def publish_summary(metrics: PipelineMetrics) -> str:
    """Publish a markdown summary artifact.

    Args:
        metrics: The pipeline metrics to publish.

    Returns:
        The markdown content.
    """
    markdown = f"""# Production Pipeline v2 Summary

**Run time:** {datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M UTC")}

| Metric | Value |
|---|---|
| Total records | {metrics.total_records} |
| Valid records | {metrics.valid_records} |
| Invalid records | {metrics.invalid_records} |
| Total value | {metrics.total_value:,.2f} |
| Regions | {", ".join(metrics.regions)} |
"""
    create_markdown_artifact(
        key="production-pipeline-v2-summary",
        markdown=markdown,
        description="Production pipeline v2 run summary",
    )
    print("Published pipeline summary artifact")
    return markdown


# ---------------------------------------------------------------------------
# Subflows
# ---------------------------------------------------------------------------


@flow(name="patterns_extract", log_prints=True)
def extract_stage() -> list[SourceRecord]:
    """Extract records from the source system.

    Returns:
        A list of source records.
    """
    raw_data: list[dict[str, Any]] = [
        {"id": 1, "name": "Alice", "value": 250.0, "region": "US"},
        {"id": 2, "name": "Bob", "value": 150.0, "region": "EU"},
        {"id": 3, "name": "Charlie", "value": 300.0, "region": "US"},
        {"id": 4, "name": "Diana", "value": 175.0, "region": "APAC"},
        {"id": 5, "name": "Eve", "value": 125.0, "region": "EU"},
    ]
    futures = extract_record.map(raw_data)
    records = [f.result() for f in futures]
    print(f"Extracted {len(records)} records")
    return records


@flow(name="patterns_validate", log_prints=True)
def validate_stage(records: list[SourceRecord]) -> list[ValidatedRecord]:
    """Validate all source records.

    Args:
        records: Source records to validate.

    Returns:
        A list of validated records (invalid records filtered out).
    """
    validated = []
    for record in records:
        result = validate_record(record)
        if result is not None:
            validated.append(result)
    print(f"Validated: {len(validated)}/{len(records)} passed")
    return validated


@flow(name="patterns_transform", log_prints=True)
def transform_stage(records: list[ValidatedRecord]) -> list[dict[str, Any]]:
    """Transform validated records.

    Args:
        records: Validated records to transform.

    Returns:
        A list of transformed record dicts.
    """
    results = []
    for record in records:
        transformed = transform_record(record)
        results.append(transformed)
    print(f"Transformed {len(results)} records")
    return results


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


@flow(name="patterns_production_pipeline_v2", log_prints=True, on_completion=[on_pipeline_completion])
def production_pipeline_v2_flow() -> None:
    """Run the Phase 3 capstone production pipeline.

    Features demonstrated:
    - Pydantic models for type-safe data passing
    - Field validators for data quality
    - Transactions for atomic operations
    - Retries on extraction tasks
    - Markdown artifacts for reporting
    - Tags for organisation
    - State hooks for observability
    - .map() for parallel extraction
    """
    logger = get_run_logger()
    logger.info("Starting production pipeline v2")

    with tags("production", "phase-3"):
        # Extract
        source_records = extract_stage()

        # Validate (inside transaction for atomicity)
        with transaction():
            validated = validate_stage(source_records)

        # Transform
        transformed = transform_stage(validated)

        # Compute metrics and publish
        metrics = compute_metrics(transformed)
        publish_summary(metrics)

    logger.info("Production pipeline v2 complete")


if __name__ == "__main__":
    production_pipeline_v2_flow()
