"""Data Transfer.

Cross-system data synchronization simulation with computed categorical
columns and transfer verification via row count and checksum.

Airflow equivalent: Generic table-to-table transfer with transformation (DAG 037).
Prefect approach:    Generate source data, transform each record (add size
                     category), transfer batch, verify, and produce summary.
"""

import datetime
import hashlib

from dotenv import load_dotenv
from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class SourceRecord(BaseModel):
    """Record in the source system."""

    id: int
    city: str
    country: str
    population: int


class DestRecord(BaseModel):
    """Record in the destination system."""

    city: str
    country: str
    population: int
    size_category: str
    transferred_at: str


class TransferVerification(BaseModel):
    """Verification result for a data transfer."""

    count_match: bool
    checksum_match: bool


class TransferResult(BaseModel):
    """Result of a data transfer operation."""

    source_count: int
    dest_count: int
    matched: bool
    categories: dict[str, int]
    checksum_match: bool


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def generate_source_data() -> list[SourceRecord]:
    """Generate deterministic source data.

    Returns:
        List of SourceRecord.
    """
    data = [
        SourceRecord(id=1, city="Oslo", country="Norway", population=700000),
        SourceRecord(id=2, city="Bergen", country="Norway", population=285000),
        SourceRecord(id=3, city="Stockholm", country="Sweden", population=975000),
        SourceRecord(id=4, city="Gothenburg", country="Sweden", population=580000),
        SourceRecord(id=5, city="Helsinki", country="Finland", population=655000),
        SourceRecord(id=6, city="Copenhagen", country="Denmark", population=800000),
        SourceRecord(id=7, city="Reykjavik", country="Iceland", population=130000),
        SourceRecord(id=8, city="Tromso", country="Norway", population=77000),
    ]
    print(f"Generated {len(data)} source records")
    return data


@task
def transform_for_dest(source: SourceRecord) -> DestRecord:
    """Transform a source record for the destination system.

    Adds a size_category based on population thresholds and a timestamp.

    Args:
        source: Source record.

    Returns:
        DestRecord.
    """
    if source.population >= 500000:
        category = "large"
    elif source.population >= 200000:
        category = "medium"
    elif source.population >= 100000:
        category = "small"
    else:
        category = "town"

    return DestRecord(
        city=source.city,
        country=source.country,
        population=source.population,
        size_category=category,
        transferred_at=datetime.datetime.now(datetime.UTC).isoformat(),
    )


@task
def transfer_batch(sources: list[SourceRecord]) -> list[DestRecord]:
    """Transform and transfer a batch of records.

    Args:
        sources: Source records.

    Returns:
        List of DestRecord.
    """
    return [transform_for_dest.fn(s) for s in sources]


@task
def verify_transfer(sources: list[SourceRecord], destinations: list[DestRecord]) -> TransferVerification:
    """Verify transfer integrity.

    Args:
        sources: Source records.
        destinations: Destination records.

    Returns:
        TransferVerification.
    """
    count_match = len(sources) == len(destinations)

    # Checksum based on city+population
    source_hash = hashlib.sha256(
        "|".join(f"{s.city}:{s.population}" for s in sorted(sources, key=lambda x: x.city)).encode()
    ).hexdigest()[:16]
    dest_hash = hashlib.sha256(
        "|".join(f"{d.city}:{d.population}" for d in sorted(destinations, key=lambda x: x.city)).encode()
    ).hexdigest()[:16]

    return TransferVerification(count_match=count_match, checksum_match=source_hash == dest_hash)


@task
def transfer_summary(
    sources: list[SourceRecord],
    destinations: list[DestRecord],
    verification: TransferVerification,
) -> TransferResult:
    """Build the final transfer result.

    Args:
        sources: Source records.
        destinations: Destination records.
        verification: Transfer verification.

    Returns:
        TransferResult.
    """
    categories: dict[str, int] = {}
    for d in destinations:
        categories[d.size_category] = categories.get(d.size_category, 0) + 1

    return TransferResult(
        source_count=len(sources),
        dest_count=len(destinations),
        matched=verification.count_match,
        categories=categories,
        checksum_match=verification.checksum_match,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="analytics_data_transfer", log_prints=True)
def data_transfer_flow() -> TransferResult:
    """Run the data transfer pipeline.

    Returns:
        TransferResult.
    """
    sources = generate_source_data()
    destinations = transfer_batch(sources)
    verification = verify_transfer(sources, destinations)
    result = transfer_summary(sources, destinations, verification)
    print(f"Transfer complete: {result.source_count} -> {result.dest_count}, match={result.matched}")
    return result


if __name__ == "__main__":
    load_dotenv()
    data_transfer_flow()
