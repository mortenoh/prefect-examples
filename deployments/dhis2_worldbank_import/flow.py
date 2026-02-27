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
from prefect_dhis2 import Dhis2Client, get_dhis2_credentials
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WORLDBANK_API_URL = "https://api.worldbank.org/v2"

DATA_ELEMENT_UID = "PfPopTotal1"
DATA_SET_UID = "PfPopDtSet1"

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class Dhis2Ref(BaseModel):
    """A DHIS2 object reference (just an id)."""

    id: str = Field(description="DHIS2 UID")


class Dhis2DataElement(BaseModel):
    """A DHIS2 data element for the metadata API."""

    id: str = Field(description="Fixed UID")
    name: str = Field(description="Display name")
    shortName: str = Field(description="Short name")
    domainType: str = Field(default="AGGREGATE", description="AGGREGATE or TRACKER")
    valueType: str = Field(default="NUMBER", description="Value type")
    aggregationType: str = Field(default="SUM", description="Aggregation type")


class Dhis2DataSetElement(BaseModel):
    """A data element assignment within a data set."""

    dataElement: Dhis2Ref = Field(description="Data element reference")


class Dhis2DataSet(BaseModel):
    """A DHIS2 data set for the metadata API."""

    id: str = Field(description="Fixed UID")
    name: str = Field(description="Display name")
    shortName: str = Field(description="Short name")
    periodType: str = Field(default="Yearly", description="Period type")
    dataSetElements: list[Dhis2DataSetElement] = Field(default_factory=list, description="Data elements in the set")
    organisationUnits: list[Dhis2Ref] = Field(default_factory=list, description="Assigned org units")


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


@task
def ensure_dhis2_metadata(client: Dhis2Client) -> dict[str, Any]:
    """Ensure the population data element and data set exist in DHIS2.

    Fetches level-1 org units, builds the metadata payload, and POSTs to
    /api/metadata.  Uses CREATE_AND_UPDATE so this is idempotent.

    Args:
        client: Authenticated DHIS2 client.

    Returns:
        Raw DHIS2 metadata import summary.
    """
    level1_ous = client.fetch_metadata("organisationUnits", fields="id,name", filters=["level:eq:1"])
    level1_refs = [Dhis2Ref(id=ou["id"]) for ou in level1_ous]

    data_element = Dhis2DataElement(
        id=DATA_ELEMENT_UID,
        name="Prefect - Population",
        shortName="Prefect - Pop",
    )
    data_set = Dhis2DataSet(
        id=DATA_SET_UID,
        name="Prefect - Population",
        shortName="Prefect - Pop",
        periodType="Yearly",
        dataSetElements=[Dhis2DataSetElement(dataElement=Dhis2Ref(id=DATA_ELEMENT_UID))],
        organisationUnits=level1_refs,
    )

    payload: dict[str, Any] = {
        "dataElements": [data_element.model_dump()],
        "dataSets": [data_set.model_dump()],
    }

    print(f"Metadata payload: {payload}")
    result = client.post_metadata(payload)
    print(f"Metadata response: {result}")
    stats = result.get("stats", {})
    print(
        f"Metadata sync: created={stats.get('created', 0)}, "
        f"updated={stats.get('updated', 0)}, "
        f"ignored={stats.get('ignored', 0)}, "
        f"org units={len(level1_refs)}"
    )
    return result


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
    print(f"DHIS2 target: {creds.base_url}")
    client = creds.get_client()

    ensure_dhis2_metadata(client)
    populations = fetch_population_data(query.iso3_codes, query.start_year, query.end_year)
    org_unit_map = resolve_org_units(client, query.iso3_codes)
    report = build_report(creds.base_url, query, populations, org_unit_map)

    create_markdown_artifact(key="dhis2-worldbank-import", markdown=report.markdown)
    return report


if __name__ == "__main__":
    load_dotenv()
    dhis2_worldbank_import_flow()
