"""Pydantic Models.

Use Pydantic models as task parameters and return types for automatic
validation, serialization, and type safety between tasks.

Airflow equivalent: XCom push/pull with complex types (needs JSON/pickle).
Prefect approach:    Pydantic models flow naturally between tasks.
"""

from typing import Any

from dotenv import load_dotenv
from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class PipelineConfig(BaseModel):
    """Configuration for the extraction pipeline."""

    source: str = "users_api"
    batch_size: int = 100
    enable_validation: bool = True


class UserRecord(BaseModel):
    """A single user record with validated fields."""

    name: str
    email: str
    age: int


class ProcessingResult(BaseModel):
    """Outcome of a processing step."""

    records: list[dict[str, Any]]
    errors: list[str]
    summary: str


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def extract_users(config: PipelineConfig) -> list[UserRecord]:
    """Extract user records based on pipeline configuration.

    Args:
        config: Pipeline configuration with source and batch settings.

    Returns:
        A list of validated UserRecord instances.
    """
    raw: list[dict[str, str | int]] = [
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
        {"name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]
    users = [UserRecord(**r) for r in raw[: config.batch_size]]  # type: ignore[arg-type]
    print(f"Extracted {len(users)} users from {config.source}")
    return users


@task
def validate_users(users: list[UserRecord]) -> ProcessingResult:
    """Validate a list of user records.

    Args:
        users: List of user records to validate.

    Returns:
        A ProcessingResult with valid records and any errors.
    """
    valid = []
    errors = []
    for user in users:
        if user.age < 0 or user.age > 150:
            errors.append(f"Invalid age for {user.name}: {user.age}")
        else:
            valid.append(user.model_dump())
    summary = f"{len(valid)} valid, {len(errors)} errors"
    print(f"Validation: {summary}")
    return ProcessingResult(records=valid, errors=errors, summary=summary)


@task
def summarize(result: ProcessingResult) -> str:
    """Produce a human-readable summary of processing results.

    Args:
        result: The processing result to summarize.

    Returns:
        A summary string.
    """
    msg = f"Pipeline complete: {result.summary} ({len(result.records)} records)"
    print(msg)
    return msg


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="patterns_pydantic_models", log_prints=True)
def pydantic_models_flow() -> None:
    """Run a pipeline using Pydantic models for type-safe data passing."""
    config = PipelineConfig(source="users_api", batch_size=10, enable_validation=True)
    users = extract_users(config)
    result = validate_users(users)
    summarize(result)


if __name__ == "__main__":
    load_dotenv()
    pydantic_models_flow()
