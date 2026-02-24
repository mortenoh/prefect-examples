"""091 -- Staged ETL Pipeline.

Simulated SQL pipeline with staging, production, and summary layers.
Data validation with is_valid flag and in-memory table operations.

Airflow equivalent: SQL-based three-layer ETL (DAGs 035-036).
Prefect approach:    Load raw -> staging -> validate + transform to production
                     -> filter valid -> compute summary stats by group.
"""

import datetime

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class RawRecord(BaseModel):
    """A single raw data record."""

    id: int
    name: str
    category: str
    value_raw: str


class StagingRecord(BaseModel):
    """Record in the staging layer."""

    id: int
    name: str
    category: str
    value_raw: str
    load_timestamp: str


class ProductionRecord(BaseModel):
    """Record in the production layer."""

    id: int
    name: str
    category: str
    value: float
    is_valid: bool
    loaded_at: str


class SummaryStats(BaseModel):
    """Summary statistics for a group."""

    group: str
    avg: float
    min_val: float
    max_val: float
    count: int


class EtlResult(BaseModel):
    """Final ETL pipeline result."""

    staging_count: int
    production_count: int
    valid_count: int
    invalid_count: int
    summary: list[SummaryStats]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def generate_raw_data() -> list[RawRecord]:
    """Generate deterministic raw data for the ETL pipeline.

    Returns:
        List of RawRecord.
    """
    data = [
        RawRecord(id=1, name="alpha", category="A", value_raw="100.5"),
        RawRecord(id=2, name="beta", category="A", value_raw="200.3"),
        RawRecord(id=3, name="gamma", category="B", value_raw="bad_value"),
        RawRecord(id=4, name="delta", category="B", value_raw="150.0"),
        RawRecord(id=5, name="epsilon", category="A", value_raw="-50.0"),
        RawRecord(id=6, name="zeta", category="C", value_raw="300.0"),
        RawRecord(id=7, name="eta", category="C", value_raw="250.7"),
        RawRecord(id=8, name="theta", category="B", value_raw="9999.0"),
        RawRecord(id=9, name="iota", category="A", value_raw="175.0"),
        RawRecord(id=10, name="kappa", category="C", value_raw=""),
    ]
    print(f"Generated {len(data)} raw records")
    return data


@task
def load_staging(raw_data: list[RawRecord]) -> list[StagingRecord]:
    """Load raw data into staging layer with timestamps.

    Args:
        raw_data: Raw data records.

    Returns:
        List of StagingRecord.
    """
    timestamp = datetime.datetime.now(datetime.UTC).isoformat()
    records: list[StagingRecord] = []
    for r in raw_data:
        records.append(
            StagingRecord(
                id=r.id,
                name=r.name,
                category=r.category,
                value_raw=r.value_raw,
                load_timestamp=timestamp,
            )
        )
    print(f"Loaded {len(records)} records to staging")
    return records


@task
def validate_and_transform(staging: list[StagingRecord]) -> list[ProductionRecord]:
    """Validate and transform staging records to production.

    Validation rules:
    - value_raw must be parseable as float
    - parsed value must be in range [0, 1000]

    Args:
        staging: Staging records.

    Returns:
        List of ProductionRecord with is_valid flag.
    """
    records: list[ProductionRecord] = []
    for s in staging:
        is_valid = True
        value = 0.0
        try:
            value = float(s.value_raw)
            if value < 0 or value > 1000:
                is_valid = False
        except (ValueError, TypeError):
            is_valid = False

        records.append(
            ProductionRecord(
                id=s.id,
                name=s.name,
                category=s.category,
                value=value if is_valid else 0.0,
                is_valid=is_valid,
                loaded_at=s.load_timestamp,
            )
        )
    valid = sum(1 for r in records if r.is_valid)
    print(f"Validated {len(records)} records: {valid} valid, {len(records) - valid} invalid")
    return records


@task
def filter_valid(production: list[ProductionRecord]) -> list[ProductionRecord]:
    """Filter to only valid production records.

    Args:
        production: All production records.

    Returns:
        Valid records only.
    """
    return [r for r in production if r.is_valid]


@task
def compute_summary(valid_records: list[ProductionRecord], group_by: str) -> list[SummaryStats]:
    """Compute summary statistics grouped by a field.

    Args:
        valid_records: Valid production records.
        group_by: Field to group by.

    Returns:
        List of SummaryStats.
    """
    groups: dict[str, list[float]] = {}
    for r in valid_records:
        key = getattr(r, group_by)
        groups.setdefault(key, []).append(r.value)

    stats: list[SummaryStats] = []
    for group_val, values in sorted(groups.items()):
        stats.append(
            SummaryStats(
                group=group_val,
                avg=round(sum(values) / len(values), 2),
                min_val=round(min(values), 2),
                max_val=round(max(values), 2),
                count=len(values),
            )
        )
    return stats


@task
def etl_report(
    staging_count: int,
    production: list[ProductionRecord],
    summary: list[SummaryStats],
) -> EtlResult:
    """Build the final ETL result.

    Args:
        staging_count: Number of staging records.
        production: All production records.
        summary: Summary statistics.

    Returns:
        EtlResult.
    """
    valid = sum(1 for r in production if r.is_valid)
    return EtlResult(
        staging_count=staging_count,
        production_count=len(production),
        valid_count=valid,
        invalid_count=len(production) - valid,
        summary=summary,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="091_staged_etl", log_prints=True)
def staged_etl_flow() -> EtlResult:
    """Run the staged ETL pipeline.

    Returns:
        EtlResult.
    """
    raw = generate_raw_data()
    staging = load_staging(raw)
    production = validate_and_transform(staging)
    valid = filter_valid(production)
    summary = compute_summary(valid, "category")
    result = etl_report(len(staging), production, summary)
    print(f"ETL complete: {result.valid_count} valid, {result.invalid_count} invalid")
    return result


if __name__ == "__main__":
    staged_etl_flow()
