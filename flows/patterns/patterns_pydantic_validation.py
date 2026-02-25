"""Pydantic Validation.

Use Pydantic field validators for data quality checks in a pipeline.

Airflow equivalent: Schema validation pipeline (DAGs 068-069).
Prefect approach:    Pydantic field_validator replaces manual schema checking.
"""

from typing import Any

from dotenv import load_dotenv
from prefect import flow, task
from pydantic import BaseModel, field_validator

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class WeatherReading(BaseModel):
    """A single weather reading with validated fields."""

    station_id: str
    temperature: float
    humidity: float

    @field_validator("temperature")
    @classmethod
    def temperature_in_range(cls, v: float) -> float:
        """Validate temperature is within a plausible range."""
        if v < -100 or v > 60:
            raise ValueError(f"Temperature {v} out of range [-100, 60]")
        return v

    @field_validator("humidity")
    @classmethod
    def humidity_in_range(cls, v: float) -> float:
        """Validate humidity is between 0 and 100."""
        if v < 0 or v > 100:
            raise ValueError(f"Humidity {v} out of range [0, 100]")
        return v


class ValidationReport(BaseModel):
    """Summary of a validation run."""

    valid_count: int
    invalid_count: int
    errors: list[str]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def generate_readings() -> list[dict[str, Any]]:
    """Generate raw weather readings, some intentionally invalid.

    Returns:
        A list of raw reading dicts.
    """
    readings = [
        {"station_id": "WX001", "temperature": 22.5, "humidity": 65.0},
        {"station_id": "WX002", "temperature": -15.0, "humidity": 80.0},
        {"station_id": "WX003", "temperature": 75.0, "humidity": 50.0},
        {"station_id": "WX004", "temperature": 30.0, "humidity": -5.0},
        {"station_id": "WX005", "temperature": 18.0, "humidity": 72.0},
    ]
    print(f"Generated {len(readings)} raw readings")
    return readings


@task
def validate_readings(raw: list[dict[str, Any]]) -> ValidationReport:
    """Validate raw readings against the WeatherReading schema.

    Args:
        raw: A list of raw reading dicts.

    Returns:
        A ValidationReport with counts and error details.
    """
    valid_count = 0
    errors = []

    for reading in raw:
        try:
            WeatherReading(**reading)
            valid_count += 1
        except Exception as e:
            errors.append(f"Station {reading.get('station_id', '?')}: {e}")

    report = ValidationReport(
        valid_count=valid_count,
        invalid_count=len(raw) - valid_count,
        errors=errors,
    )
    print(f"Validation: {report.valid_count} valid, {report.invalid_count} invalid")
    return report


@task
def publish_report(report: ValidationReport) -> str:
    """Publish a validation report summary.

    Args:
        report: The validation report to publish.

    Returns:
        A formatted summary string.
    """
    lines = [f"Validation Report: {report.valid_count} valid, {report.invalid_count} invalid"]
    for error in report.errors:
        lines.append(f"  - {error}")
    summary = "\n".join(lines)
    print(summary)
    return summary


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="patterns_pydantic_validation", log_prints=True)
def pydantic_validation_flow() -> None:
    """Run a data validation pipeline using Pydantic field validators."""
    raw = generate_readings()
    report = validate_readings(raw)
    publish_report(report)


if __name__ == "__main__":
    load_dotenv()
    pydantic_validation_flow()
