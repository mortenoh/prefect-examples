"""Flow of Flows.

Orchestrate multiple flows from a parent flow for modular pipelines.

Airflow equivalent: TriggerDagRunOperator.
Prefect approach:    subflow calls; run_deployment() for deployed flows.
"""

from __future__ import annotations

from dotenv import load_dotenv
from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class Record(BaseModel):
    """A single data record with an optional normalised value."""

    id: int
    value: int
    normalised: float = 0.0


class RawData(BaseModel):
    """Raw data payload from the source system."""

    source: str
    records: list[Record]


class ProcessedData(BaseModel):
    """Processed data payload with aggregates and enriched records."""

    source: str
    record_count: int
    total_value: int
    records: list[Record]


@task
def fetch_raw_data() -> RawData:
    """Simulate fetching raw data from an external source.

    Returns:
        A RawData payload with records and metadata.
    """
    data = RawData(
        source="api-v2",
        records=[
            Record(id=1, value=10),
            Record(id=2, value=20),
            Record(id=3, value=30),
        ],
    )
    print(f"Fetched {len(data.records)} raw records from {data.source}")
    return data


@task
def process_data(raw: RawData) -> ProcessedData:
    """Clean and aggregate raw records.

    Args:
        raw: The raw data payload from ingestion.

    Returns:
        ProcessedData with totals and normalised records.
    """
    total = sum(r.value for r in raw.records)
    records = [
        r.model_copy(update={"normalised": r.value / total})
        for r in raw.records
    ]
    processed = ProcessedData(
        source=raw.source,
        record_count=len(records),
        total_value=total,
        records=records,
    )
    print(f"Processed {processed.record_count} records, total value: {total}")
    return processed


@task
def build_report(data: ProcessedData) -> str:
    """Generate a human-readable report summary.

    Args:
        data: The processed data payload.

    Returns:
        A formatted report string.
    """
    report = f"Report -- source: {data.source}, records: {data.record_count}, total: {data.total_value}"
    print(report)
    return report


# ---------------------------------------------------------------------------
# Subflows
# ---------------------------------------------------------------------------


@flow(name="basics_ingest", log_prints=True)
def ingest_flow() -> RawData:
    """Ingest raw data from the source system."""
    return fetch_raw_data()


@flow(name="basics_transform", log_prints=True)
def transform_flow(raw: RawData) -> ProcessedData:
    """Transform and enrich raw data.

    Args:
        raw: RawData payload from ingestion.
    """
    return process_data(raw)


@flow(name="basics_report", log_prints=True)
def report_flow(data: ProcessedData) -> str:
    """Generate a summary report from processed data.

    Args:
        data: ProcessedData payload.
    """
    return build_report(data)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


@flow(name="basics_flow_of_flows", log_prints=True)
def orchestrator() -> None:
    """Run ingest -> transform -> report as chained subflows."""
    raw = ingest_flow()
    processed = transform_flow(raw)
    summary = report_flow(processed)
    print(f"Pipeline complete: {summary}")


if __name__ == "__main__":
    load_dotenv()
    orchestrator()
