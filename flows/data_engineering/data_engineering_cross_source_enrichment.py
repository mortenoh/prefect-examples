"""Cross-Source Enrichment.

Joins data from three simulated API sources with graceful degradation:
if an enrichment source fails, the record continues with partial data.

Airflow equivalent: Cross-API enrichment (DAGs 090, 092).
Prefect approach:    Multiple enrichment tasks per record, None return
                     for failures, completeness tracking.
"""

from typing import Any

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class BaseRecord(BaseModel):
    """A base record to be enriched."""

    id: int
    name: str
    region: str


class EnrichedRecord(BaseModel):
    """A record after enrichment from multiple sources."""

    id: int
    name: str
    region: str
    demographic: dict[str, Any] | None = None
    financial: dict[str, Any] | None = None
    geographic: dict[str, Any] | None = None
    enrichment_completeness: float = 0.0


class EnrichmentReport(BaseModel):
    """Summary of enrichment results."""

    total_records: int
    fully_enriched: int
    partially_enriched: int
    demographic_rate: float
    financial_rate: float
    geographic_rate: float
    avg_completeness: float


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def fetch_base_records() -> list[BaseRecord]:
    """Fetch base records to enrich.

    Returns:
        List of BaseRecord objects.
    """
    regions = ["US", "EU", "APAC", "LATAM"]
    records = [BaseRecord(id=i, name=f"Entity_{i}", region=regions[i % len(regions)]) for i in range(1, 11)]
    print(f"Fetched {len(records)} base records")
    return records


@task
def enrich_from_demographics(record_id: int) -> dict[str, Any] | None:
    """Simulate demographic enrichment. Fails for certain IDs.

    Args:
        record_id: Record ID to enrich.

    Returns:
        Demographic dict or None if unavailable.
    """
    if record_id % 4 == 0:
        return None
    return {
        "age_group": "25-34" if record_id % 3 == 0 else "35-44",
        "income_bracket": "medium" if record_id % 2 == 0 else "high",
    }


@task
def enrich_from_financials(record_id: int) -> dict[str, Any] | None:
    """Simulate financial enrichment. Fails for certain IDs.

    Args:
        record_id: Record ID to enrich.

    Returns:
        Financial dict or None if unavailable.
    """
    if record_id % 5 == 0:
        return None
    return {
        "credit_score": 600 + (record_id * 17) % 200,
        "account_type": "premium" if record_id % 3 == 0 else "standard",
    }


@task
def enrich_from_geography(record_id: int) -> dict[str, Any] | None:
    """Simulate geographic enrichment. Fails for certain IDs.

    Args:
        record_id: Record ID to enrich.

    Returns:
        Geographic dict or None if unavailable.
    """
    if record_id % 7 == 0:
        return None
    return {
        "timezone": f"UTC+{record_id % 12}",
        "currency": ["USD", "EUR", "GBP", "JPY"][record_id % 4],
    }


@task
def merge_enrichments(
    base: BaseRecord,
    demo: dict[str, Any] | None,
    fin: dict[str, Any] | None,
    geo: dict[str, Any] | None,
) -> EnrichedRecord:
    """Merge enrichment data into a single record.

    Args:
        base: Base record.
        demo: Demographic enrichment (or None).
        fin: Financial enrichment (or None).
        geo: Geographic enrichment (or None).

    Returns:
        EnrichedRecord with completeness score.
    """
    sources_available = sum(1 for s in [demo, fin, geo] if s is not None)
    completeness = sources_available / 3.0
    return EnrichedRecord(
        id=base.id,
        name=base.name,
        region=base.region,
        demographic=demo,
        financial=fin,
        geographic=geo,
        enrichment_completeness=round(completeness, 2),
    )


@task
def enrichment_summary(records: list[EnrichedRecord]) -> EnrichmentReport:
    """Compute enrichment statistics.

    Args:
        records: All enriched records.

    Returns:
        EnrichmentReport with rates per source.
    """
    total = len(records)
    fully = sum(1 for r in records if r.enrichment_completeness == 1.0)
    partially = sum(1 for r in records if 0 < r.enrichment_completeness < 1.0)
    demo_rate = sum(1 for r in records if r.demographic is not None) / total
    fin_rate = sum(1 for r in records if r.financial is not None) / total
    geo_rate = sum(1 for r in records if r.geographic is not None) / total
    avg_comp = sum(r.enrichment_completeness for r in records) / total

    return EnrichmentReport(
        total_records=total,
        fully_enriched=fully,
        partially_enriched=partially,
        demographic_rate=round(demo_rate, 2),
        financial_rate=round(fin_rate, 2),
        geographic_rate=round(geo_rate, 2),
        avg_completeness=round(avg_comp, 2),
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="data_engineering_cross_source_enrichment", log_prints=True)
def cross_source_enrichment_flow() -> EnrichmentReport:
    """Enrich base records from three simulated sources with graceful degradation.

    Returns:
        EnrichmentReport.
    """
    base_records = fetch_base_records()

    enriched = []
    for record in base_records:
        demo = enrich_from_demographics(record.id)
        fin = enrich_from_financials(record.id)
        geo = enrich_from_geography(record.id)
        merged = merge_enrichments(record, demo, fin, geo)
        enriched.append(merged)

    report = enrichment_summary(enriched)
    print(f"Enrichment complete: {report.fully_enriched} fully, {report.partially_enriched} partially enriched")
    return report


if __name__ == "__main__":
    cross_source_enrichment_flow()
