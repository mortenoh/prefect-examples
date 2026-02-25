"""Flow of Flows.

Orchestrate multiple flows from a parent flow for modular pipelines.

Airflow equivalent: TriggerDagRunOperator.
Prefect approach:    subflow calls; run_deployment() for deployed flows.
"""

from __future__ import annotations

from typing import Any

from dotenv import load_dotenv
from prefect import flow, task


@task
def fetch_raw_data() -> dict[str, Any]:
    """Simulate fetching raw data from an external source.

    Returns:
        A raw data payload with records and metadata.
    """
    data = {
        "source": "api-v2",
        "records": [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
            {"id": 3, "value": 30},
        ],
    }
    print(f"Fetched {len(data['records'])} raw records from {data['source']}")
    return data


@task
def process_data(raw: dict[str, Any]) -> dict[str, Any]:
    """Clean and aggregate raw records.

    Args:
        raw: The raw data payload from ingestion.

    Returns:
        Processed payload with totals and cleaned records.
    """
    records = raw["records"]
    total = sum(r["value"] for r in records)
    processed = {
        "source": raw["source"],
        "record_count": len(records),
        "total_value": total,
        "records": [{**r, "normalised": r["value"] / total} for r in records],
    }
    print(f"Processed {processed['record_count']} records, total value: {total}")
    return processed


@task
def build_report(data: dict[str, Any]) -> str:
    """Generate a human-readable report summary.

    Args:
        data: The processed data payload.

    Returns:
        A formatted report string.
    """
    report = f"Report — source: {data['source']}, records: {data['record_count']}, total: {data['total_value']}"
    print(report)
    return report


# ---------------------------------------------------------------------------
# Subflows
# ---------------------------------------------------------------------------


@flow(name="basics_ingest", log_prints=True)
def ingest_flow() -> dict[str, Any]:
    """Ingest raw data from the source system."""
    return fetch_raw_data()


@flow(name="basics_transform", log_prints=True)
def transform_flow(raw: dict[str, Any]) -> dict[str, Any]:
    """Transform and enrich raw data.

    Args:
        raw: Raw data payload from ingestion.
    """
    return process_data(raw)


@flow(name="basics_report", log_prints=True)
def report_flow(data: dict[str, Any]) -> str:
    """Generate a summary report from processed data.

    Args:
        data: Processed data payload.
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
