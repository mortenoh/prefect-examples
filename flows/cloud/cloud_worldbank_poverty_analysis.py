"""World Bank Poverty Headcount Analysis.

Analyze poverty headcount ratio ($2.15/day, indicator SI.POV.DDAY) across
multiple countries, handling missing and sparse data common with poverty
indicators.  Demonstrates robust API response handling with the World Bank API.

Airflow equivalent: PythonOperator with per-country API calls and aggregation.
Prefect approach:   tasks with .map() for parallel fetching, retry, and markdown artifact.
"""

from __future__ import annotations

from typing import Any

import httpx
from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WORLDBANK_API_URL = "https://api.worldbank.org/v2"

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class PovertyQuery(BaseModel):
    """Parameters for multi-country poverty analysis."""

    iso3_codes: list[str] = Field(
        default=["ETH", "KEN", "TZA", "NGA", "IND", "BGD", "VNM"],
        description="Country ISO3 codes to query",
    )
    start_year: int = Field(default=2010, description="Start year for data range")
    end_year: int = Field(default=2023, description="End year for data range")


class CountryPoverty(BaseModel):
    """Poverty headcount data for a single country."""

    iso3: str = Field(description="Country ISO3 code")
    country_name: str = Field(default="", description="Country display name")
    latest_value: float | None = Field(default=None, description="Most recent poverty headcount ratio (%)")
    latest_year: int | None = Field(default=None, description="Year of the most recent data point")
    data_points: int = Field(default=0, description="Number of non-null data points available")
    trend: str = Field(
        default="unknown",
        description="improving, worsening, stable, or unknown",
    )


class PovertyReport(BaseModel):
    """Multi-country poverty analysis report."""

    countries: list[CountryPoverty] = Field(description="Per-country poverty data")
    countries_with_data: int = Field(description="Number of countries with at least one data point")
    markdown: str = Field(description="Markdown report content")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=[2, 5])
def fetch_poverty_data(iso3: str, start_year: int, end_year: int) -> CountryPoverty:
    """Fetch poverty headcount ratio for a single country.

    Args:
        iso3: Country ISO3 code.
        start_year: Start year of the data range.
        end_year: End year of the data range.

    Returns:
        CountryPoverty with latest value, trend, and data-point count.
    """
    url = (
        f"{WORLDBANK_API_URL}/country/{iso3}/indicator/SI.POV.DDAY?date={start_year}:{end_year}&format=json&per_page=50"
    )

    with httpx.Client(timeout=30) as client:
        resp = client.get(url)
        resp.raise_for_status()

    payload: Any = resp.json()

    # World Bank returns [paging_info, [data_entries, ...]]
    if not isinstance(payload, list) or len(payload) < 2 or not payload[1]:
        print(f"{iso3}: no poverty data returned")
        return CountryPoverty(iso3=iso3)

    entries: list[dict[str, Any]] = payload[1]

    # Extract country name from first entry
    country_name = ""
    if entries:
        country_name = entries[0].get("country", {}).get("value", "")

    # Filter to non-null values and collect (year, value) pairs
    data: list[tuple[int, float]] = []
    for entry in entries:
        value = entry.get("value")
        if value is not None:
            data.append((int(entry.get("date", 0)), float(value)))

    if not data:
        print(f"{iso3} ({country_name}): all values null")
        return CountryPoverty(iso3=iso3, country_name=country_name)

    # Sort by year ascending
    data.sort(key=lambda x: x[0])

    latest_year, latest_value = data[-1]
    _, earliest_value = data[0]

    # Determine trend
    trend = "unknown"
    if len(data) >= 2:
        diff = latest_value - earliest_value
        if diff < -1:
            trend = "improving"
        elif diff > 1:
            trend = "worsening"
        else:
            trend = "stable"

    print(f"{iso3} ({country_name}): {latest_value:.1f}% ({latest_year}), {len(data)} points, trend={trend}")
    return CountryPoverty(
        iso3=iso3,
        country_name=country_name,
        latest_value=latest_value,
        latest_year=latest_year,
        data_points=len(data),
        trend=trend,
    )


@task
def aggregate_poverty(countries: list[CountryPoverty]) -> list[CountryPoverty]:
    """Sort countries: those with data first (by rate ascending), then without.

    Args:
        countries: Raw per-country poverty data.

    Returns:
        Sorted list of CountryPoverty.
    """
    with_data = [c for c in countries if c.latest_value is not None]
    without_data = [c for c in countries if c.latest_value is None]

    with_data.sort(key=lambda c: c.latest_value or 0.0)
    sorted_countries = with_data + without_data

    print(f"Aggregated {len(countries)} countries: {len(with_data)} with data, {len(without_data)} without")
    return sorted_countries


@task
def build_poverty_report(countries: list[CountryPoverty]) -> str:
    """Build a markdown poverty analysis report.

    Args:
        countries: Sorted list of CountryPoverty entries.

    Returns:
        Markdown string with table and summary.
    """
    with_data = [c for c in countries if c.latest_value is not None]

    lines = [
        "## World Bank Poverty Headcount Analysis",
        "",
        "**Indicator:** SI.POV.DDAY (Poverty headcount ratio at $2.15/day)",
        "",
        "| Country | Latest Rate (%) | Year | Trend | Data Points |",
        "|---------|----------------|------|-------|-------------|",
    ]

    for c in countries:
        rate = f"{c.latest_value:.1f}%" if c.latest_value is not None else "N/A"
        year = str(c.latest_year) if c.latest_year is not None else "N/A"
        lines.append(f"| {c.country_name or c.iso3} | {rate} | {year} | {c.trend} | {c.data_points} |")

    lines.extend(["", "### Summary", ""])
    lines.append(f"- **Countries with data:** {len(with_data)}/{len(countries)}")

    if with_data:
        avg_rate = sum(c.latest_value for c in with_data if c.latest_value is not None) / len(with_data)
        lines.append(f"- **Average poverty rate:** {avg_rate:.1f}%")

        best = min(with_data, key=lambda c: c.latest_value or 0.0)
        worst = max(with_data, key=lambda c: c.latest_value or 0.0)
        lines.append(f"- **Best performer:** {best.country_name or best.iso3} ({best.latest_value:.1f}%)")
        lines.append(f"- **Worst performer:** {worst.country_name or worst.iso3} ({worst.latest_value:.1f}%)")

    markdown = "\n".join(lines)
    print(f"Built poverty report: {len(lines)} lines")
    return markdown


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="cloud_worldbank_poverty_analysis", log_prints=True)
def worldbank_poverty_analysis_flow(
    query: PovertyQuery | None = None,
) -> PovertyReport:
    """Analyze poverty headcount ratio across multiple countries.

    Args:
        query: Optional PovertyQuery with iso3_codes and year range.

    Returns:
        PovertyReport with sorted countries, data coverage, and markdown.
    """
    if query is None:
        query = PovertyQuery()

    futures = fetch_poverty_data.map(
        query.iso3_codes,
        start_year=query.start_year,
        end_year=query.end_year,
    )
    raw_countries = [f.result() for f in futures]

    countries = aggregate_poverty(raw_countries)
    markdown = build_poverty_report(countries)

    create_markdown_artifact(key="worldbank-poverty-analysis", markdown=markdown)

    countries_with_data = sum(1 for c in countries if c.latest_value is not None)

    return PovertyReport(
        countries=countries,
        countries_with_data=countries_with_data,
        markdown=markdown,
    )


if __name__ == "__main__":
    load_dotenv()
    worldbank_poverty_analysis_flow()
