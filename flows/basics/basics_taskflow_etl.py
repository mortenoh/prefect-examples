"""Taskflow / ETL.

Classic extract-transform-load pipeline wired through return values.

Airflow equivalent: @task (TaskFlow API).
Prefect approach:    Prefect is natively taskflow-first.
"""

from __future__ import annotations

from typing import Any

from prefect import flow, task


@task
def extract() -> dict[str, Any]:
    """Simulate data extraction from an external source.

    Returns:
        Raw payload with a user list and a timestamp.
    """
    raw = {
        "users": [
            {"name": "Alice", "age": 30, "active": True},
            {"name": "Bob", "age": 17, "active": True},
            {"name": "Charlie", "age": 25, "active": False},
        ],
        "timestamp": "2024-01-15T10:30:00Z",
    }
    print(f"Extracted {len(raw['users'])} users")
    return raw


@task
def transform(raw: dict[str, Any]) -> dict[str, Any]:
    """Filter to active adults and upper-case their names.

    Args:
        raw: The raw payload from the extract step.

    Returns:
        Transformed payload with filtered and cleaned users.
    """
    users = [{**u, "name": u["name"].upper()} for u in raw["users"] if u["active"] and u["age"] >= 18]
    transformed = {"users": users, "timestamp": raw["timestamp"]}
    print(f"Transformed down to {len(users)} users")
    return transformed


@task
def load(data: dict[str, Any]) -> str:
    """Persist the transformed data (simulated).

    Args:
        data: The transformed payload to load.

    Returns:
        A summary string describing what was loaded.
    """
    summary = f"Loaded {len(data['users'])} users (ts={data['timestamp']})"
    print(summary)
    return summary


@flow(name="basics_taskflow_etl", log_prints=True)
def taskflow_etl_flow() -> None:
    """Run a simple ETL pipeline: extract -> transform -> load."""
    raw = extract()
    transformed = transform(raw)
    load(transformed)


if __name__ == "__main__":
    taskflow_etl_flow()
