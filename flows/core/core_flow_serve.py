"""Flow Serve.

Demonstrate the simplest deployment method: flow.serve().

Airflow equivalent: DAG placed in dags/ folder, picked up by scheduler.
Prefect approach:    flow.serve() creates a lightweight deployment that
                     runs locally. Pass cron= or interval= for scheduling.

Notes:
    flow.serve() blocks and runs a long-lived process. For production,
    use flow.deploy() with work pools for infrastructure-level isolation.

    # Example usage (not auto-executed to avoid blocking):
    # flow_fn.serve(name="037-flow-serve", cron="*/5 * * * *")
"""

import datetime
from typing import Any

from prefect import flow, task


@task
def extract_data() -> list[dict[str, Any]]:
    """Extract sample data records.

    Returns:
        A list of raw data records.
    """
    records = [
        {"id": 1, "value": 100, "timestamp": datetime.datetime.now(datetime.UTC).isoformat()},
        {"id": 2, "value": 200, "timestamp": datetime.datetime.now(datetime.UTC).isoformat()},
        {"id": 3, "value": 150, "timestamp": datetime.datetime.now(datetime.UTC).isoformat()},
    ]
    print(f"Extracted {len(records)} records")
    return records


@task
def transform_data(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Transform records by normalising values.

    Args:
        records: Raw data records.

    Returns:
        Transformed records with normalised values.
    """
    max_val = max(r["value"] for r in records) if records else 1
    transformed = [{**r, "normalised": round(r["value"] / max_val, 2)} for r in records]
    print(f"Transformed {len(transformed)} records")
    return transformed


@task
def load_data(records: list[dict[str, Any]]) -> str:
    """Load transformed records.

    Args:
        records: Transformed data records.

    Returns:
        A load summary string.
    """
    msg = f"Loaded {len(records)} records"
    print(msg)
    return msg


@flow(name="core_flow_serve", log_prints=True)
def flow_serve_flow() -> None:
    """Standard ETL pipeline suitable for flow.serve() deployment.

    In production, deploy with:
        flow_serve_flow.serve(name="037-flow-serve", cron="*/5 * * * *")
    """
    raw = extract_data()
    transformed = transform_data(raw)
    load_data(transformed)


if __name__ == "__main__":
    # Run the flow directly (not served) for local testing.
    # To serve: flow_serve_flow.serve(name="037-flow-serve", cron="*/5 * * * *")
    flow_serve_flow()
