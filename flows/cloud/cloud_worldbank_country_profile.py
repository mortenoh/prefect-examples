"""World Bank Country Profile Dashboard.

Fetch multiple World Bank indicators for a single country to build a
comprehensive country profile dashboard.  Uses .map() to query indicators
in parallel.

Airflow equivalent: PythonOperator + HttpHook per indicator.
Prefect approach:   httpx task with .map(), markdown artifact.
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

DEFAULT_INDICATORS = [
    "SP.POP.TOTL",  # Population
    "NY.GDP.MKTP.CD",  # GDP (current US$)
    "NY.GDP.PCAP.CD",  # GDP per capita
    "SP.DYN.LE00.IN",  # Life expectancy
    "SE.ADT.LITR.ZS",  # Literacy rate
    "SH.DYN.MORT",  # Under-5 mortality rate (per 1,000)
]

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class ProfileQuery(BaseModel):
    """Parameters for a country profile query."""

    iso3: str = Field(default="ETH", description="Country ISO3 code")
    year: int = Field(default=2023, description="Year for indicator data")
    indicators: list[str] = Field(
        default=DEFAULT_INDICATORS,
        description="World Bank indicator codes to fetch",
    )


class IndicatorValue(BaseModel):
    """Single indicator data point."""

    indicator_code: str = Field(description="World Bank indicator code")
    indicator_name: str = Field(default="", description="Human-readable indicator name")
    value: float | None = Field(default=None, description="Indicator value")
    year: int = Field(default=0, description="Data year")


class CountryProfile(BaseModel):
    """Assembled country profile with all indicators."""

    iso3: str = Field(description="Country ISO3 code")
    country_name: str = Field(default="", description="Country display name")
    year: int = Field(description="Requested data year")
    indicators: list[IndicatorValue] = Field(description="Fetched indicator values")
    markdown: str = Field(description="Markdown report content")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=[2, 5])
def fetch_indicator(iso3: str, indicator: str, year: int) -> IndicatorValue:
    """Fetch a single indicator value from the World Bank API.

    Args:
        iso3: Country ISO3 code.
        indicator: World Bank indicator code.
        year: Data year.

    Returns:
        IndicatorValue with the fetched data point.
    """
    url = f"{WORLDBANK_API_URL}/country/{iso3}/indicator/{indicator}"
    params = {"date": str(year), "format": "json"}
    with httpx.Client(timeout=30) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()

    body: Any = resp.json()

    if not isinstance(body, list) or len(body) < 2 or not body[1]:
        print(f"{iso3}/{indicator}: no data for {year}")
        return IndicatorValue(indicator_code=indicator, year=year)

    entry = body[1][0]
    indicator_name = entry.get("indicator", {}).get("value", "")
    raw_value = entry.get("value")
    data_year = int(entry.get("date", year))

    result = IndicatorValue(
        indicator_code=indicator,
        indicator_name=indicator_name,
        value=float(raw_value) if raw_value is not None else None,
        year=data_year,
    )
    print(f"{iso3}/{indicator}: {result.value} ({data_year})")
    return result


@task
def build_country_profile(
    iso3: str,
    year: int,
    indicators: list[IndicatorValue],
) -> CountryProfile:
    """Build a markdown country profile from fetched indicators.

    Args:
        iso3: Country ISO3 code.
        year: Requested data year.
        indicators: List of fetched indicator values.

    Returns:
        CountryProfile with assembled markdown.
    """
    country_name = ""
    if indicators:
        country_name = iso3  # fallback
    # Derive country name from the first indicator that has a name
    for iv in indicators:
        if iv.indicator_name:
            country_name = iso3
            break

    lines = [
        f"## Country Profile: {country_name} ({year})",
        "",
        "| Indicator | Value |",
        "|-----------|-------|",
    ]

    for iv in indicators:
        formatted = _format_value(iv)
        lines.append(f"| {iv.indicator_name or iv.indicator_code} | {formatted} |")

    lines.extend(["", "### Summary", ""])
    with_data = [iv for iv in indicators if iv.value is not None]
    lines.append(f"- **Indicators with data:** {len(with_data)}/{len(indicators)}")

    markdown = "\n".join(lines)
    print(f"Built profile for {country_name}: {len(indicators)} indicators")

    return CountryProfile(
        iso3=iso3,
        country_name=country_name,
        year=year,
        indicators=indicators,
        markdown=markdown,
    )


def _format_value(iv: IndicatorValue) -> str:
    """Format an indicator value for display.

    Args:
        iv: The indicator value to format.

    Returns:
        Formatted string.
    """
    if iv.value is None:
        return "N/A"
    if iv.indicator_code.endswith(".ZS"):
        return f"{iv.value:.1f}%"
    if iv.value >= 1_000_000_000:
        return f"${iv.value / 1e9:.1f}B"
    if iv.value >= 1_000_000:
        return f"{iv.value:,.0f}"
    return f"{iv.value:,.1f}"


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="cloud_worldbank_country_profile", log_prints=True)
def worldbank_country_profile_flow(
    query: ProfileQuery | None = None,
) -> CountryProfile:
    """Fetch World Bank indicators and build a country profile dashboard.

    Args:
        query: Optional ProfileQuery with iso3, year, and indicator list.

    Returns:
        CountryProfile with markdown and indicator data.
    """
    if query is None:
        query = ProfileQuery()

    futures = fetch_indicator.map(
        iso3=query.iso3,
        indicator=query.indicators,
        year=query.year,
    )
    indicator_values = [f.result() for f in futures]

    profile = build_country_profile(query.iso3, query.year, indicator_values)

    create_markdown_artifact(key="worldbank-country-profile", markdown=profile.markdown)

    return profile


if __name__ == "__main__":
    load_dotenv()
    worldbank_country_profile_flow()
