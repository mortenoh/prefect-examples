"""World Bank Indicator Time-Series.

Fetch a World Bank indicator for a single country over a year range,
compute year-over-year growth rates, and identify peak/trough years.

Airflow equivalent: PythonOperator chain with HttpHook for paginated API.
Prefect approach:   sequential tasks with growth rate analysis and markdown artifact.
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


class TimeseriesQuery(BaseModel):
    """Parameters for World Bank indicator time-series query."""

    iso3: str = Field(default="ETH")
    indicator: str = Field(default="SP.POP.TOTL", description="World Bank indicator code")
    start_year: int = Field(default=2000)
    end_year: int = Field(default=2023)


class YearValue(BaseModel):
    """A single year's indicator value."""

    year: int
    value: float


class GrowthRate(BaseModel):
    """Year-over-year growth rate for an indicator value."""

    year: int
    value: float
    growth_pct: float


class TimeseriesReport(BaseModel):
    """Full time-series analysis report."""

    iso3: str
    indicator: str
    data_points: list[GrowthRate]
    peak_growth: GrowthRate | None
    trough_growth: GrowthRate | None
    markdown: str


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=[2, 5])
def fetch_indicator_timeseries(iso3: str, indicator: str, start_year: int, end_year: int) -> list[YearValue]:
    """Fetch indicator data from the World Bank API for a country and year range.

    Args:
        iso3: Country ISO3 code.
        indicator: World Bank indicator code.
        start_year: First year of the range.
        end_year: Last year of the range.

    Returns:
        List of YearValue entries sorted by year ascending.
    """
    url = f"{WORLDBANK_API_URL}/country/{iso3}/indicator/{indicator}"
    params = {
        "date": f"{start_year}:{end_year}",
        "format": "json",
        "per_page": "100",
    }
    with httpx.Client(timeout=30) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()

    body = resp.json()

    # World Bank API returns [metadata, data_list] -- data is the second element
    if not isinstance(body, list) or len(body) < 2:
        print(f"No data returned for {iso3}/{indicator}")
        return []

    raw_items = body[1] or []

    results: list[YearValue] = []
    for item in raw_items:
        if item.get("value") is not None:
            results.append(
                YearValue(
                    year=int(item["date"]),
                    value=float(item["value"]),
                )
            )

    results.sort(key=lambda r: r.year)
    print(f"Fetched {len(results)} data points for {iso3}/{indicator} ({start_year}-{end_year})")
    return results


@task
def compute_growth_rates(data: list[YearValue]) -> list[GrowthRate]:
    """Compute year-over-year growth rates from consecutive data points.

    For each consecutive pair, growth is (current - previous) / previous * 100.

    Args:
        data: Sorted list of YearValue entries.

    Returns:
        List of GrowthRate entries (one fewer than input).
    """
    if len(data) < 2:
        return []

    rates: list[GrowthRate] = []
    for i in range(1, len(data)):
        prev = data[i - 1]
        curr = data[i]
        growth = (curr.value - prev.value) / prev.value * 100 if prev.value != 0 else 0.0
        rates.append(
            GrowthRate(
                year=curr.year,
                value=curr.value,
                growth_pct=round(growth, 4),
            )
        )

    print(f"Computed {len(rates)} growth rate entries")
    return rates


@task
def build_timeseries_report(iso3: str, indicator: str, data: list[GrowthRate]) -> TimeseriesReport:
    """Build a markdown report with indicator values, growth rates, and summary.

    Args:
        iso3: Country ISO3 code.
        indicator: World Bank indicator code.
        data: List of GrowthRate entries.

    Returns:
        TimeseriesReport with markdown content, peak, and trough.
    """
    peak: GrowthRate | None = None
    trough: GrowthRate | None = None
    if data:
        peak = max(data, key=lambda g: g.growth_pct)
        trough = min(data, key=lambda g: g.growth_pct)

    lines = [
        f"## World Bank Indicator Time-Series: {iso3} / {indicator}",
        "",
        f"**Data points:** {len(data)}",
    ]

    if data:
        avg_growth = sum(g.growth_pct for g in data) / len(data)
        lines.append(f"**Average growth:** {avg_growth:.4f}%")
        if peak is not None:
            lines.append(f"**Peak growth year:** {peak.year} ({peak.growth_pct:.4f}%)")
        if trough is not None:
            lines.append(f"**Trough growth year:** {trough.year} ({trough.growth_pct:.4f}%)")

    lines.extend(
        [
            "",
            "### Data",
            "",
            "| Year | Value | Growth % |",
            "|------|-------|----------|",
        ]
    )
    for g in data:
        lines.append(f"| {g.year} | {g.value:,.0f} | {g.growth_pct:.4f}% |")

    lines.extend(
        [
            "",
            "### Summary",
            "",
            f"- **Total data points:** {len(data)}",
        ]
    )
    if data:
        avg_growth = sum(g.growth_pct for g in data) / len(data)
        lines.append(f"- **Average growth:** {avg_growth:.4f}%")
        if peak is not None:
            lines.append(f"- **Peak year:** {peak.year}")
        if trough is not None:
            lines.append(f"- **Trough year:** {trough.year}")

    markdown = "\n".join(lines)
    return TimeseriesReport(
        iso3=iso3,
        indicator=indicator,
        data_points=data,
        peak_growth=peak,
        trough_growth=trough,
        markdown=markdown,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="cloud_worldbank_indicator_timeseries", log_prints=True)
def worldbank_indicator_timeseries_flow(query: TimeseriesQuery | None = None) -> TimeseriesReport:
    """Fetch a World Bank indicator, compute growth rates, and identify extremes.

    Args:
        query: Optional TimeseriesQuery with iso3, indicator, start_year, end_year.

    Returns:
        TimeseriesReport with time-series data and growth analysis.
    """
    if query is None:
        query = TimeseriesQuery()

    raw_data = fetch_indicator_timeseries(query.iso3, query.indicator, query.start_year, query.end_year)
    growth_rates = compute_growth_rates(raw_data)
    report = build_timeseries_report(query.iso3, query.indicator, growth_rates)

    create_markdown_artifact(key="worldbank-indicator-timeseries", markdown=report.markdown)

    return report


if __name__ == "__main__":
    load_dotenv()
    worldbank_indicator_timeseries_flow()
