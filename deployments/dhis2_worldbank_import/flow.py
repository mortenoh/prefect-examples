"""DHIS2 World Bank Population Report -- deployment-ready flow.

Fetches total population (SP.POP.TOTL) from the World Bank API for a set
of countries and years, resolves matching DHIS2 organisation units, and
produces a markdown report.

Three ways to register this deployment:

1. CLI::

    cd deployments/dhis2_worldbank_import
    prefect deploy --all

2. Declarative (prefect.yaml in this directory)::

    See prefect.yaml

3. Python::

    python deployments/dhis2_worldbank_import/deploy.py
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel, Field

from prefect_examples.dhis2 import (
    Dhis2Client,
    get_dhis2_credentials,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WORLDBANK_API_URL = "https://api.worldbank.org/v2"

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class PopulationQuery(BaseModel):
    """Parameters for a World Bank population report."""

    iso3_codes: list[str] = Field(description="ISO3 country codes to fetch")
    start_year: int = Field(description="First year of data range (inclusive)")
    end_year: int = Field(description="Last year of data range (inclusive)")


class CountryPopulation(BaseModel):
    """A single population data point from the World Bank."""

    iso3: str = Field(description="Country ISO3 code")
    country_name: str = Field(default="", description="Country display name")
    year: int = Field(description="Data year")
    population: int = Field(description="Total population")


class OrgUnitMapping(BaseModel):
    """Mapping from ISO3 code to a DHIS2 organisation unit."""

    iso3: str = Field(description="Country ISO3 code")
    org_unit_uid: str = Field(description="DHIS2 organisation unit UID")
    org_unit_name: str = Field(default="", description="Organisation unit display name")


class PopulationReport(BaseModel):
    """Summary report of World Bank population data and DHIS2 org unit mapping."""

    dhis2_url: str = Field(description="Target DHIS2 instance URL")
    record_count: int = Field(default=0, description="Population records fetched")
    org_units_resolved: int = Field(default=0, description="Org units matched in DHIS2")
    org_units_requested: int = Field(default=0, description="Org units requested")
    markdown: str = Field(default="", description="Markdown summary")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=[2, 5])
def fetch_population_data(
    iso3_codes: list[str],
    start_year: int,
    end_year: int,
) -> list[CountryPopulation]:
    """Fetch total population from the World Bank for multiple countries.

    Uses indicator SP.POP.TOTL with semicolon-joined country codes for a
    single batch request.

    Args:
        iso3_codes: List of ISO3 country codes.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        List of CountryPopulation entries (null values filtered out).
    """
    codes = ";".join(iso3_codes)
    url = f"{WORLDBANK_API_URL}/country/{codes}/indicator/SP.POP.TOTL"
    num_expected = len(iso3_codes) * (end_year - start_year + 1)
    params: dict[str, str] = {
        "date": f"{start_year}:{end_year}",
        "format": "json",
        "per_page": str(max(num_expected, 100)),
    }

    with httpx.Client(timeout=30) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()

    payload: Any = resp.json()

    if not isinstance(payload, list) or len(payload) < 2 or not payload[1]:
        print(f"No population data returned for {codes} ({start_year}-{end_year})")
        return []

    results: list[CountryPopulation] = []
    for entry in payload[1]:
        value = entry.get("value")
        if value is None:
            continue
        results.append(
            CountryPopulation(
                iso3=entry.get("countryiso3code", ""),
                country_name=entry.get("country", {}).get("value", ""),
                year=int(entry.get("date", 0)),
                population=int(value),
            )
        )

    print(f"Fetched population data: {len(results)} records for {len(iso3_codes)} countries")
    return results


@task
def resolve_org_units(
    client: Dhis2Client,
    iso3_codes: list[str],
) -> dict[str, OrgUnitMapping]:
    """Resolve ISO3 codes to DHIS2 organisation unit UIDs.

    Args:
        client: Authenticated DHIS2 client.
        iso3_codes: ISO3 codes to resolve.

    Returns:
        Dict mapping ISO3 code to OrgUnitMapping.
    """
    raw = client.fetch_organisation_units_by_code(iso3_codes)
    mapping: dict[str, OrgUnitMapping] = {}
    for ou in raw:
        code = ou.get("code", "")
        mapping[code] = OrgUnitMapping(
            iso3=code,
            org_unit_uid=ou["id"],
            org_unit_name=ou.get("name", ""),
        )

    resolved = set(mapping.keys())
    missing = set(iso3_codes) - resolved
    if missing:
        logger.warning("Unresolved ISO3 codes (no matching org unit): %s", sorted(missing))
        print(f"WARNING: {len(missing)} ISO3 codes not found in DHIS2: {sorted(missing)}")

    print(f"Resolved {len(mapping)}/{len(iso3_codes)} org units")
    return mapping


@task
def build_report(
    dhis2_url: str,
    query: PopulationQuery,
    populations: list[CountryPopulation],
    org_unit_map: dict[str, OrgUnitMapping],
) -> PopulationReport:
    """Build a markdown report from population data and org unit mappings.

    Args:
        dhis2_url: DHIS2 instance base URL.
        query: The original query parameters.
        populations: Fetched population records.
        org_unit_map: Resolved org unit mappings.

    Returns:
        PopulationReport with markdown summary.
    """
    lines = [
        "## World Bank Population Report",
        "",
        f"**DHIS2 target:** {dhis2_url}",
        f"**Countries:** {', '.join(query.iso3_codes)}",
        f"**Period:** {query.start_year}-{query.end_year}",
        "",
    ]

    if populations:
        lines.append("### Population Data")
        lines.append("")
        lines.append("| Country | ISO3 | Year | Population |")
        lines.append("|---------|------|------|-----------|")
        for pop in sorted(populations, key=lambda p: (p.iso3, p.year)):
            lines.append(f"| {pop.country_name} | {pop.iso3} | {pop.year} | {pop.population:,} |")
        lines.append("")
    else:
        lines.append("*No population data returned from World Bank.*")
        lines.append("")

    lines.append("### DHIS2 Org Unit Mapping")
    lines.append("")
    if org_unit_map:
        lines.append("| ISO3 | Org Unit | UID |")
        lines.append("|------|----------|-----|")
        for code in sorted(org_unit_map):
            ou = org_unit_map[code]
            lines.append(f"| {ou.iso3} | {ou.org_unit_name} | {ou.org_unit_uid} |")
    else:
        lines.append("*No matching org units found in DHIS2.*")

    lines.append("")
    lines.append(f"**Resolved:** {len(org_unit_map)}/{len(query.iso3_codes)} org units")

    markdown = "\n".join(lines)

    print(f"Report: {len(populations)} records, {len(org_unit_map)}/{len(query.iso3_codes)} org units resolved")

    return PopulationReport(
        dhis2_url=dhis2_url,
        record_count=len(populations),
        org_units_resolved=len(org_unit_map),
        org_units_requested=len(query.iso3_codes),
        markdown=markdown,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="dhis2_worldbank_import", log_prints=True)
def dhis2_worldbank_import_flow(
    query: PopulationQuery | None = None,
) -> PopulationReport:
    """Fetch World Bank population data and report with DHIS2 org unit mapping.

    Args:
        query: Query parameters. Uses demo defaults if not provided.

    Returns:
        PopulationReport with markdown.
    """
    if query is None:
        query = PopulationQuery(
            iso3_codes=["LAO"],
            start_year=2020,
            end_year=2023,
        )

    creds = get_dhis2_credentials()
    client = creds.get_client()

    populations = fetch_population_data(query.iso3_codes, query.start_year, query.end_year)
    org_unit_map = resolve_org_units(client, query.iso3_codes)
    report = build_report(creds.base_url, query, populations, org_unit_map)

    create_markdown_artifact(key="dhis2-worldbank-import", markdown=report.markdown)
    return report


if __name__ == "__main__":
    load_dotenv()
    dhis2_worldbank_import_flow()
