"""WorldPop Country Comparison.

Compare population metadata across multiple countries for a given year.
Queries the catalog API for each country in parallel using task.map().

Airflow equivalent: PythonOperator in a loop or dynamic task mapping.
Prefect approach:   .map() for parallel API calls with markdown artifact.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WORLDPOP_CATALOG_URL = "https://hub.worldpop.org/rest/data"

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class ComparisonQuery(BaseModel):
    """Parameters for multi-country comparison."""

    iso3_codes: list[str] = Field(description="List of country ISO3 codes to compare")
    dataset: str = Field(default="pop", description="Top-level dataset ID")
    subdataset: str = Field(default="wpgp", description="Sub-dataset ID")
    year: int = Field(default=2020, description="Population year to filter")


class CountryMetadata(BaseModel):
    """Metadata for a single country from the catalog API."""

    iso3: str = Field(description="Country ISO3 code")
    title: str = Field(default="", description="Dataset title")
    popyear: str = Field(default="", description="Population year")
    doi: str = Field(default="", description="DOI reference")
    data_available: bool = Field(default=False, description="Whether data was found")


class ComparisonReport(BaseModel):
    """Multi-country comparison report."""

    countries_queried: int = Field(description="Number of countries queried")
    countries_with_data: int = Field(description="Number of countries with data")
    records: list[CountryMetadata] = Field(description="Per-country metadata")
    markdown: str = Field(description="Markdown comparison report")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=[2, 5])
def fetch_country_metadata(
    iso3: str,
    dataset: str,
    subdataset: str,
    year: int,
) -> CountryMetadata:
    """Fetch catalog metadata for a single country and year.

    Args:
        iso3: Country ISO3 code.
        dataset: Top-level dataset ID.
        subdataset: Sub-dataset ID.
        year: Population year to filter.

    Returns:
        CountryMetadata with data availability.
    """
    url = f"{WORLDPOP_CATALOG_URL}/{dataset}/{subdataset}"
    params = {"iso3": iso3}
    with httpx.Client(timeout=30) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()

    body: dict[str, Any] = resp.json()
    items: list[dict[str, Any]] = body.get("data", [])

    # Filter by year
    year_str = str(year)
    matching = [item for item in items if str(item.get("popyear", "")) == year_str]

    if matching:
        item = matching[0]
        result = CountryMetadata(
            iso3=iso3,
            title=item.get("title", ""),
            popyear=str(item.get("popyear", "")),
            doi=item.get("doi", ""),
            data_available=True,
        )
    else:
        result = CountryMetadata(iso3=iso3, data_available=False)

    print(f"{iso3}: {'found' if result.data_available else 'no data'} for year {year}")
    return result


@task
def aggregate_comparison(records: list[CountryMetadata]) -> list[CountryMetadata]:
    """Aggregate and sort country metadata records.

    Args:
        records: List of CountryMetadata from parallel queries.

    Returns:
        Sorted list of CountryMetadata.
    """
    sorted_records = sorted(records, key=lambda r: r.iso3)
    available = sum(1 for r in sorted_records if r.data_available)
    print(f"Aggregated {len(sorted_records)} countries, {available} with data")
    return sorted_records


@task
def build_comparison_report(records: list[CountryMetadata], query: ComparisonQuery) -> ComparisonReport:
    """Build a markdown comparison table from country metadata.

    Args:
        records: Aggregated country metadata.
        query: Original comparison query.

    Returns:
        ComparisonReport with markdown content.
    """
    countries_with_data = sum(1 for r in records if r.data_available)

    lines = [
        f"## WorldPop Country Comparison ({query.year})",
        "",
        f"**Dataset:** {query.dataset}/{query.subdataset}",
        f"**Countries queried:** {len(records)}",
        f"**Countries with data:** {countries_with_data}",
        "",
        "| Country | Year | Title | DOI | Available |",
        "|---------|------|-------|-----|-----------|",
    ]
    for r in records:
        available = "Yes" if r.data_available else "No"
        lines.append(f"| {r.iso3} | {r.popyear} | {r.title} | {r.doi} | {available} |")

    markdown = "\n".join(lines)
    return ComparisonReport(
        countries_queried=len(records),
        countries_with_data=countries_with_data,
        records=records,
        markdown=markdown,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="cloud_worldpop_country_comparison", log_prints=True)
def worldpop_country_comparison_flow(query: ComparisonQuery | None = None) -> ComparisonReport:
    """Compare population metadata across multiple countries using .map().

    Args:
        query: Optional ComparisonQuery with iso3_codes, dataset, subdataset, year.

    Returns:
        ComparisonReport with comparison table.
    """
    if query is None:
        query = ComparisonQuery(iso3_codes=["ETH", "KEN", "TZA", "UGA", "RWA"])

    # Use .map() for parallel country queries
    futures = fetch_country_metadata.map(
        query.iso3_codes,
        dataset=query.dataset,
        subdataset=query.subdataset,
        year=query.year,
    )
    records = [f.result() for f in futures]

    sorted_records = aggregate_comparison(records)
    report = build_comparison_report(sorted_records, query)

    create_markdown_artifact(key="worldpop-country-comparison", markdown=report.markdown)

    return report


if __name__ == "__main__":
    load_dotenv()
    worldpop_country_comparison_flow()
