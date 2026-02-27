"""World Bank GDP Comparison.

Compare GDP (current US$) across multiple countries for a given year
using the World Bank API indicator NY.GDP.MKTP.CD.

Airflow equivalent: PythonOperator with batch API call and post-processing.
Prefect approach:   tasks with retry for API calls and markdown artifact.
"""

from __future__ import annotations

import logging

import httpx
from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WORLDBANK_API_URL = "https://api.worldbank.org/v2"

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class GdpComparisonQuery(BaseModel):
    """Parameters for multi-country GDP comparison."""

    iso3_codes: list[str] = Field(description="List of country ISO3 codes to compare")
    year: int = Field(default=2023, description="Year of GDP data to fetch")


class CountryGdp(BaseModel):
    """GDP data for a single country."""

    iso3: str = Field(description="Country ISO3 code")
    country_name: str = Field(default="", description="Country display name")
    gdp_usd: float = Field(default=0.0, description="GDP in current US dollars")
    year: int = Field(default=0, description="Data year")


class GdpComparisonReport(BaseModel):
    """Multi-country GDP comparison report."""

    countries: list[CountryGdp] = Field(description="Ranked list of countries by GDP")
    total_countries: int = Field(description="Number of countries in the report")
    markdown: str = Field(description="Markdown comparison report")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=[2, 5])
def fetch_gdp_data(iso3_codes: list[str], year: int) -> list[CountryGdp]:
    """Fetch GDP data for multiple countries in a single batch API call.

    Args:
        iso3_codes: List of ISO3 country codes.
        year: Year of GDP data to fetch.

    Returns:
        List of CountryGdp with parsed GDP values.
    """
    codes = ";".join(iso3_codes)
    url = f"{WORLDBANK_API_URL}/country/{codes}/indicator/NY.GDP.MKTP.CD"
    params = {"date": str(year), "format": "json", "per_page": str(len(iso3_codes))}

    with httpx.Client(timeout=30) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()

    payload = resp.json()

    # World Bank returns [paging_info, [data_entries, ...]]
    if not isinstance(payload, list) or len(payload) < 2 or not payload[1]:
        print(f"No GDP data returned for {codes} in {year}")
        return []

    entries = payload[1]
    results: list[CountryGdp] = []
    for entry in entries:
        gdp_value = entry.get("value")
        results.append(
            CountryGdp(
                iso3=entry.get("countryiso3code", ""),
                country_name=entry.get("country", {}).get("value", ""),
                gdp_usd=float(gdp_value) if gdp_value is not None else 0.0,
                year=int(entry.get("date", 0)),
            )
        )

    print(f"Fetched GDP data for {len(results)} countries ({year})")
    return results


@task
def rank_by_gdp(countries: list[CountryGdp]) -> list[CountryGdp]:
    """Sort countries by GDP in descending order.

    Args:
        countries: List of CountryGdp entries.

    Returns:
        Sorted list with highest GDP first.
    """
    ranked = sorted(countries, key=lambda c: c.gdp_usd, reverse=True)
    for i, c in enumerate(ranked, start=1):
        print(f"  #{i} {c.country_name} ({c.iso3}): ${c.gdp_usd:,.0f}")
    return ranked


@task
def build_gdp_report(countries: list[CountryGdp], query: GdpComparisonQuery) -> str:
    """Build a markdown GDP comparison table.

    Args:
        countries: Ranked list of CountryGdp entries.
        query: Original comparison query.

    Returns:
        Markdown string with GDP comparison table and summary.
    """
    total_gdp = sum(c.gdp_usd for c in countries)
    largest = countries[0] if countries else None
    smallest = countries[-1] if countries else None

    lines = [
        f"## World Bank GDP Comparison ({query.year})",
        "",
        "**Indicator:** NY.GDP.MKTP.CD (GDP, current US$)",
        f"**Countries:** {len(countries)}",
        "",
        "| Rank | Country | ISO3 | GDP (US$) |",
        "|------|---------|------|-----------|",
    ]
    for i, c in enumerate(countries, start=1):
        lines.append(f"| {i} | {c.country_name} | {c.iso3} | {c.gdp_usd:,.0f} |")

    lines.append("")
    lines.append("### Summary")
    lines.append("")
    lines.append(f"- **Total GDP:** ${total_gdp:,.0f}")
    if largest:
        lines.append(f"- **Largest economy:** {largest.country_name} (${largest.gdp_usd:,.0f})")
    if smallest:
        lines.append(f"- **Smallest economy:** {smallest.country_name} (${smallest.gdp_usd:,.0f})")

    markdown = "\n".join(lines)
    print(f"Built GDP report with {len(countries)} countries")
    return markdown


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="cloud_worldbank_gdp_comparison", log_prints=True)
def worldbank_gdp_comparison_flow(query: GdpComparisonQuery | None = None) -> GdpComparisonReport:
    """Compare GDP across multiple countries for a given year.

    Args:
        query: Optional GdpComparisonQuery with iso3_codes and year.

    Returns:
        GdpComparisonReport with ranked countries and markdown table.
    """
    if query is None:
        query = GdpComparisonQuery(iso3_codes=["USA", "CHN", "JPN", "DEU", "IND", "GBR", "FRA"])

    countries = fetch_gdp_data(query.iso3_codes, query.year)
    ranked = rank_by_gdp(countries)
    markdown = build_gdp_report(ranked, query)

    create_markdown_artifact(key="worldbank-gdp-comparison", markdown=markdown)

    return GdpComparisonReport(
        countries=ranked,
        total_countries=len(ranked),
        markdown=markdown,
    )


if __name__ == "__main__":
    load_dotenv()
    worldbank_gdp_comparison_flow()
