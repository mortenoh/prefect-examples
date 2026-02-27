"""WorldPop Country Population Report with Slack Notification.

Fetch population metadata for multiple countries from the WorldPop catalog
API, enrich with population numbers from the World Bank API, build a
markdown report, and send it to Slack via webhook.

Airflow equivalent: PythonOperator + HttpHook + SlackWebhookOperator.
Prefect approach:   httpx tasks with .map(), markdown artifact, SlackWebhook block.
"""

from __future__ import annotations

import os
from typing import Any

import httpx
from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.blocks.notifications import SlackWebhook
from pydantic import BaseModel, Field, SecretStr

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WORLDPOP_CATALOG_URL = "https://hub.worldpop.org/rest/data"
WORLDBANK_API_URL = "https://api.worldbank.org/v2"

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class CountryReportQuery(BaseModel):
    """Parameters for multi-country population report."""

    iso3_codes: list[str] = Field(description="Country ISO3 codes to include")
    dataset: str = Field(default="pop", description="Top-level dataset ID")
    subdataset: str = Field(default="wpgp", description="Sub-dataset ID")
    year: int = Field(default=2024, description="Year for population data")


class CountryPopulationInfo(BaseModel):
    """Population metadata for a single country."""

    iso3: str = Field(description="Country ISO3 code")
    title: str = Field(default="", description="Dataset title")
    years_available: int = Field(default=0, description="Number of years with data")
    latest_year: int | None = Field(default=None, description="Most recent year")
    earliest_year: int | None = Field(default=None, description="Earliest year")
    doi: str = Field(default="", description="DOI of the latest dataset")
    population: int = Field(default=0, description="Total population for the requested year")


class PopulationReport(BaseModel):
    """Final population report with markdown and notification status."""

    countries: list[CountryPopulationInfo] = Field(description="Per-country data")
    total_countries: int = Field(description="Number of countries included")
    markdown: str = Field(description="Markdown report content")
    slack_sent: bool = Field(default=False, description="Whether Slack notification was sent")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=[2, 5])
def fetch_country_data(iso3: str, dataset: str, subdataset: str) -> CountryPopulationInfo:
    """Fetch population metadata for a single country.

    Args:
        iso3: Country ISO3 code.
        dataset: Top-level dataset ID.
        subdataset: Sub-dataset ID.

    Returns:
        CountryPopulationInfo with year range and availability.
    """
    url = f"{WORLDPOP_CATALOG_URL}/{dataset}/{subdataset}"
    params = {"iso3": iso3}
    with httpx.Client(timeout=30) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()

    body: dict[str, Any] = resp.json()
    items: list[dict[str, Any]] = body.get("data", [])

    if not items:
        print(f"{iso3}: no data found")
        return CountryPopulationInfo(iso3=iso3)

    years = [int(item["popyear"]) for item in items if item.get("popyear") is not None]
    years.sort()

    latest_item = next((item for item in items if item.get("popyear") == years[-1]), items[0])

    info = CountryPopulationInfo(
        iso3=iso3,
        title=latest_item.get("title", ""),
        years_available=len(years),
        latest_year=years[-1] if years else None,
        earliest_year=years[0] if years else None,
        doi=latest_item.get("doi", ""),
    )
    print(f"{iso3}: {info.years_available} years ({info.earliest_year}-{info.latest_year})")
    return info


@task(retries=2, retry_delay_seconds=[2, 5])
def fetch_population(iso3_codes: list[str], year: int) -> dict[str, int]:
    """Fetch population numbers from the World Bank API.

    Args:
        iso3_codes: Country ISO3 codes.
        year: Year for population data.

    Returns:
        Mapping of ISO3 code to population count.
    """
    codes = ";".join(iso3_codes)
    url = f"{WORLDBANK_API_URL}/country/{codes}/indicator/SP.POP.TOTL"
    params = {"date": str(year), "format": "json", "per_page": str(len(iso3_codes))}
    with httpx.Client(timeout=30) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()

    body = resp.json()
    result: dict[str, int] = {}
    if isinstance(body, list) and len(body) > 1:
        for entry in body[1]:
            iso3 = entry.get("countryiso3code", "")
            value = entry.get("value")
            if iso3 and value is not None:
                result[iso3] = int(value)

    print(f"Fetched population for {len(result)}/{len(iso3_codes)} countries (year={year})")
    return result


@task
def transform_results(countries: list[CountryPopulationInfo]) -> list[CountryPopulationInfo]:
    """Sort and enrich country results.

    Args:
        countries: Raw country data from parallel queries.

    Returns:
        Sorted list of CountryPopulationInfo.
    """
    sorted_countries = sorted(countries, key=lambda c: c.population, reverse=True)
    total_pop = sum(c.population for c in sorted_countries)
    print(f"Transformed {len(sorted_countries)} countries, total population {total_pop:,}")
    return sorted_countries


@task
def build_report(countries: list[CountryPopulationInfo]) -> str:
    """Build a markdown report from country population data.

    Args:
        countries: Sorted country data.

    Returns:
        Markdown string.
    """
    # Build both a markdown version (for Prefect artifact) and a Slack
    # version using Slack mrkdwn syntax with aligned plain-text table.
    # The markdown field stores the Prefect-friendly version; the Slack
    # formatter in send_slack_notification uses build_slack_message().

    lines = [
        "## WorldPop Population Report",
        "",
        f"**Countries:** {len(countries)}",
        "",
        "| Country | Population | Range | Latest Title |",
        "|---------|-----------|-------|--------------|",
    ]
    for c in countries:
        year_range = f"{c.earliest_year}-{c.latest_year}" if c.earliest_year else "N/A"
        lines.append(f"| {c.iso3} | {c.population:,} | {year_range} | {c.title} |")

    lines.extend(["", "### Summary", ""])

    with_data = [c for c in countries if c.years_available > 0]
    if with_data:
        total_pop = sum(c.population for c in with_data)
        lines.append(f"- **Countries with data:** {len(with_data)}/{len(countries)}")
        lines.append(f"- **Total population:** {total_pop:,}")
        latest = max(with_data, key=lambda c: c.latest_year or 0)
        lines.append(f"- **Most recent data:** {latest.iso3} ({latest.latest_year})")

    markdown = "\n".join(lines)
    print(f"Built report: {len(lines)} lines")
    return markdown


def build_slack_message(countries: list[CountryPopulationInfo]) -> str:
    """Build a Slack-native mrkdwn message from country data.

    Uses Slack mrkdwn syntax: *bold*, aligned code block for the table.

    Args:
        countries: Sorted country data.

    Returns:
        Slack mrkdwn formatted string.
    """
    with_data = [c for c in countries if c.years_available > 0]

    parts: list[str] = []

    # Build aligned plain-text table inside a code block
    header = f"{'Country':<9} {'Population':>14}   {'Range':<9}"
    separator = f"{'-' * 9} {'-' * 14}   {'-' * 9}"
    table_lines = [header, separator]
    for c in countries:
        year_range = f"{c.earliest_year}-{c.latest_year}" if c.earliest_year else "N/A"
        table_lines.append(f"{c.iso3:<9} {c.population:>14,}   {year_range:<9}")
    parts.append("```" + "\n".join(table_lines) + "```")

    # Summary using Slack mrkdwn
    parts.append("")
    if with_data:
        total_pop = sum(c.population for c in with_data)
        latest = max(with_data, key=lambda c: c.latest_year or 0)
        parts.append(f"*Countries with data:* {len(with_data)}/{len(countries)}")
        parts.append(f"*Total population:* {total_pop:,}")
        parts.append(f"*Most recent data:* {latest.iso3} ({latest.latest_year})")

    return "\n".join(parts)


@task
def send_slack_notification(countries: list[CountryPopulationInfo]) -> bool:
    """Send the report to Slack if SLACK_WEBHOOK_URL is configured.

    Args:
        countries: Country data to format for Slack.

    Returns:
        True if notification was sent, False otherwise.
    """
    slack_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not slack_url:
        print("SLACK_WEBHOOK_URL not set, skipping notification")
        return False

    message = build_slack_message(countries)
    slack = SlackWebhook(url=SecretStr(slack_url))
    slack.notify(body=message, subject="WorldPop Population Report")
    print("Slack notification sent")
    return True


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="cloud_worldpop_country_report", log_prints=True)
def worldpop_country_report_flow(query: CountryReportQuery | None = None) -> PopulationReport:
    """Fetch population data for multiple countries and send a Slack report.

    Args:
        query: Optional CountryReportQuery with iso3_codes, dataset, subdataset.

    Returns:
        PopulationReport with markdown and Slack status.
    """
    if query is None:
        query = CountryReportQuery(iso3_codes=["ETH", "KEN", "TZA", "NGA", "VNM", "NPL", "THA"])

    futures = fetch_country_data.map(
        query.iso3_codes,
        dataset=query.dataset,
        subdataset=query.subdataset,
    )
    raw_countries = [f.result() for f in futures]

    pop_data = fetch_population(query.iso3_codes, query.year)
    for c in raw_countries:
        c.population = pop_data.get(c.iso3, 0)

    countries = transform_results(raw_countries)
    markdown = build_report(countries)

    create_markdown_artifact(key="worldpop-country-report", markdown=markdown)

    slack_sent = send_slack_notification(countries)

    return PopulationReport(
        countries=countries,
        total_countries=len(countries),
        markdown=markdown,
        slack_sent=slack_sent,
    )


if __name__ == "__main__":
    load_dotenv()
    worldpop_country_report_flow()
