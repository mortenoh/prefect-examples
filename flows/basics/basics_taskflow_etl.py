"""Taskflow / ETL.

Classic extract-transform-load pipeline wired through return values.

Airflow equivalent: @task (TaskFlow API).
Prefect approach:    Prefect is natively taskflow-first.
"""

from __future__ import annotations

from dotenv import load_dotenv
from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class User(BaseModel):
    """A single user record."""

    name: str
    age: int
    active: bool


class ExtractPayload(BaseModel):
    """Payload returned by the extract step and consumed by transform/load."""

    users: list[User]
    timestamp: str


@task
def extract() -> ExtractPayload:
    """Simulate data extraction from an external source.

    Returns:
        Raw payload with a user list and a timestamp.
    """
    raw = ExtractPayload(
        users=[
            User(name="Alice", age=30, active=True),
            User(name="Bob", age=17, active=True),
            User(name="Charlie", age=25, active=False),
        ],
        timestamp="2024-01-15T10:30:00Z",
    )
    print(f"Extracted {len(raw.users)} users")
    return raw


@task
def transform(raw: ExtractPayload) -> ExtractPayload:
    """Filter to active adults and upper-case their names.

    Args:
        raw: The raw payload from the extract step.

    Returns:
        Transformed payload with filtered and cleaned users.
    """
    users = [
        user.model_copy(update={"name": user.name.upper()}) for user in raw.users if user.active and user.age >= 18
    ]
    transformed = ExtractPayload(users=users, timestamp=raw.timestamp)
    print(f"Transformed down to {len(users)} users")
    return transformed


@task
def load(data: ExtractPayload) -> str:
    """Persist the transformed data (simulated).

    Args:
        data: The transformed payload to load.

    Returns:
        A summary string describing what was loaded.
    """
    summary = f"Loaded {len(data.users)} users (ts={data.timestamp})"
    print(summary)
    return summary


@flow(name="basics_taskflow_etl", log_prints=True)
def taskflow_etl_flow() -> None:
    """Run a simple ETL pipeline: extract -> transform -> load."""
    raw = extract()
    transformed = transform(raw)
    load(transformed)


if __name__ == "__main__":
    load_dotenv()
    taskflow_etl_flow()
