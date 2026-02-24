"""046 -- Error Handling ETL.

Quarantine pattern: good rows pass through, bad rows are captured with reasons.

Airflow equivalent: Error handling with quarantine (DAG 066).
Prefect approach:    Try/except in tasks with Pydantic result models.
"""

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class QuarantineResult(BaseModel):
    """Result of processing with quarantine tracking."""

    good_records: list[dict]
    bad_records: list[dict]
    errors: list[str]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def generate_data() -> list[dict]:
    """Generate sample data including some invalid records.

    Returns:
        A list of dicts, some with intentional data quality issues.
    """
    data = [
        {"id": 1, "name": "Alice", "value": 100},
        {"id": 2, "name": "Bob", "value": -50},
        {"id": 3, "name": "", "value": 75},
        {"id": 4, "name": "Diana", "value": 200},
        {"id": 5, "name": "Eve", "value": None},
    ]
    print(f"Generated {len(data)} records")
    return data


@task
def process_with_quarantine(records: list[dict]) -> QuarantineResult:
    """Process records, quarantining invalid ones.

    Args:
        records: Raw records to process.

    Returns:
        A QuarantineResult with good, bad, and error details.
    """
    good = []
    bad = []
    errors = []

    for record in records:
        try:
            if not record.get("name"):
                raise ValueError(f"Record {record.get('id')}: missing name")
            if record.get("value") is None:
                raise ValueError(f"Record {record.get('id')}: missing value")
            if record.get("value", 0) < 0:
                raise ValueError(f"Record {record.get('id')}: negative value")
            good.append(record)
        except ValueError as e:
            bad.append(record)
            errors.append(str(e))

    print(f"Processed: {len(good)} good, {len(bad)} quarantined")
    return QuarantineResult(good_records=good, bad_records=bad, errors=errors)


@task
def report_quarantine(result: QuarantineResult) -> str:
    """Generate a quarantine report.

    Args:
        result: The quarantine processing result.

    Returns:
        A formatted report string.
    """
    lines = [
        f"Quarantine Report: {len(result.good_records)} good, {len(result.bad_records)} bad",
    ]
    for error in result.errors:
        lines.append(f"  - {error}")
    report = "\n".join(lines)
    print(report)
    return report


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="046_error_handling_etl", log_prints=True)
def error_handling_etl_flow() -> None:
    """Run an ETL pipeline with quarantine error handling."""
    data = generate_data()
    result = process_with_quarantine(data)
    report_quarantine(result)


if __name__ == "__main__":
    error_handling_etl_flow()
