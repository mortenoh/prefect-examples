"""051 -- Testable Flow Patterns.

Separate business logic from Prefect wiring for maximum testability.

Airflow equivalent: Thin DAG wiring, logic in modules (DAGs 077-078).
Prefect approach:    Pure functions for logic, thin @task wrappers.
"""

from prefect import flow, task

# ---------------------------------------------------------------------------
# Pure business logic (no Prefect imports needed)
# ---------------------------------------------------------------------------


def _extract_records() -> list[dict]:
    """Extract raw records from the source.

    Returns:
        A list of raw record dicts.
    """
    return [
        {"id": 1, "name": "Alice", "score": 88},
        {"id": 2, "name": "Bob", "score": 95},
        {"id": 3, "name": "Charlie", "score": 72},
    ]


def _validate_record(record: dict) -> dict:
    """Validate a single record.

    Args:
        record: A raw record dict.

    Returns:
        The record with a 'valid' flag.

    Raises:
        ValueError: If the record is missing required fields.
    """
    if not record.get("name"):
        raise ValueError(f"Record {record.get('id')}: missing name")
    if "score" not in record:
        raise ValueError(f"Record {record.get('id')}: missing score")
    return {**record, "valid": True}


def _transform_record(record: dict) -> dict:
    """Apply business transformations to a validated record.

    Args:
        record: A validated record dict.

    Returns:
        The record with a grade field added.
    """
    score = record.get("score", 0)
    if score >= 90:
        grade = "A"
    elif score >= 80:
        grade = "B"
    elif score >= 70:
        grade = "C"
    else:
        grade = "F"
    return {**record, "grade": grade}


# ---------------------------------------------------------------------------
# Thin @task wrappers
# ---------------------------------------------------------------------------


@task
def extract() -> list[dict]:
    """Extract records (thin wrapper over pure function).

    Returns:
        A list of raw record dicts.
    """
    records = _extract_records()
    print(f"Extracted {len(records)} records")
    return records


@task
def validate(record: dict) -> dict:
    """Validate a record (thin wrapper over pure function).

    Args:
        record: A raw record dict.

    Returns:
        The validated record.
    """
    return _validate_record(record)


@task
def transform(record: dict) -> dict:
    """Transform a record (thin wrapper over pure function).

    Args:
        record: A validated record dict.

    Returns:
        The transformed record.
    """
    return _transform_record(record)


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="051_testable_flow_patterns", log_prints=True)
def testable_flow_patterns_flow() -> None:
    """Run a pipeline with cleanly separated business logic."""
    records = extract()
    for record in records:
        validated = validate(record)
        transformed = transform(validated)
        print(f"Processed: {transformed['name']} -> {transformed['grade']}")


if __name__ == "__main__":
    testable_flow_patterns_flow()
