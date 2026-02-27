"""DHIS2 World Bank Population Import -- deployment-ready flow.

Fetches total population (SP.POP.TOTL) from the World Bank API for a set
of countries and years, then imports the values into a DHIS2 data element
via POST /api/dataValueSets.

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
    Dhis2ImportSummary,
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


class ImportQuery(BaseModel):
    """Parameters for a World Bank -> DHIS2 population import."""

    iso3_codes: list[str] = Field(description="ISO3 country codes to import")
    start_year: int = Field(description="First year of data range (inclusive)")
    end_year: int = Field(description="Last year of data range (inclusive)")
    data_element_uid: str = Field(description="DHIS2 data element UID for population")


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


class DataValue(BaseModel):
    """A single DHIS2 data value (camelCase to match the DHIS2 API)."""

    dataElement: str = Field(description="Data element UID")
    period: str = Field(description="Period in DHIS2 format (e.g. '2023')")
    orgUnit: str = Field(description="Organisation unit UID")
    value: str = Field(description="Data value")


class ImportResult(BaseModel):
    """Summary of the DHIS2 import operation."""

    status: str = Field(description="DHIS2 import status")
    imported: int = Field(default=0, description="Number of values imported")
    updated: int = Field(default=0, description="Number of values updated")
    ignored: int = Field(default=0, description="Number of values ignored")
    total_submitted: int = Field(default=0, description="Total data values submitted")
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
def build_data_values(
    populations: list[CountryPopulation],
    org_unit_map: dict[str, OrgUnitMapping],
    data_element_uid: str,
) -> list[DataValue]:
    """Transform population records into DHIS2 data values.

    Entries whose ISO3 code has no matching org unit are skipped.

    Args:
        populations: World Bank population records.
        org_unit_map: ISO3 -> OrgUnitMapping dict.
        data_element_uid: Target DHIS2 data element UID.

    Returns:
        List of DataValue objects ready for import.
    """
    values: list[DataValue] = []
    skipped = 0
    for pop in populations:
        ou = org_unit_map.get(pop.iso3)
        if ou is None:
            skipped += 1
            continue
        values.append(
            DataValue(
                dataElement=data_element_uid,
                period=str(pop.year),
                orgUnit=ou.org_unit_uid,
                value=str(pop.population),
            )
        )

    if skipped:
        print(f"Skipped {skipped} records (no org unit match)")
    print(f"Built {len(values)} data values for import")
    return values


@task(retries=2, retry_delay_seconds=[2, 5])
def import_data_values(
    client: Dhis2Client,
    data_values: list[DataValue],
) -> ImportResult:
    """POST data values to DHIS2 and return an import summary.

    Args:
        client: Authenticated DHIS2 client.
        data_values: List of DataValue objects.

    Returns:
        ImportResult with counts and markdown summary.
    """
    payload = {"dataValues": [dv.model_dump() for dv in data_values]}
    raw = client.post_data_values(payload)
    summary = Dhis2ImportSummary.model_validate(raw)

    counts = summary.import_count
    lines = [
        "## DHIS2 Population Import Result",
        "",
        f"- **Status:** {summary.status}",
        f"- **Imported:** {counts.imported}",
        f"- **Updated:** {counts.updated}",
        f"- **Ignored:** {counts.ignored}",
        f"- **Deleted:** {counts.deleted}",
        f"- **Total submitted:** {len(data_values)}",
    ]
    if summary.description:
        lines.append(f"- **Description:** {summary.description}")
    markdown = "\n".join(lines)

    print(
        f"Import complete: status={summary.status}, "
        f"imported={counts.imported}, updated={counts.updated}, ignored={counts.ignored}"
    )

    return ImportResult(
        status=summary.status,
        imported=counts.imported,
        updated=counts.updated,
        ignored=counts.ignored,
        total_submitted=len(data_values),
        markdown=markdown,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="dhis2_worldbank_import", log_prints=True)
def dhis2_worldbank_import_flow(query: ImportQuery | None = None) -> ImportResult:
    """Fetch World Bank population data and import into DHIS2.

    Args:
        query: Import parameters. Uses demo defaults if not provided.

    Returns:
        ImportResult with counts and markdown.
    """
    if query is None:
        query = ImportQuery(
            iso3_codes=["ETH", "KEN", "MOZ"],
            start_year=2020,
            end_year=2023,
            data_element_uid="FnYCr2EAzWS",
        )

    client = get_dhis2_credentials().get_client()

    populations = fetch_population_data(query.iso3_codes, query.start_year, query.end_year)
    org_unit_map = resolve_org_units(client, query.iso3_codes)
    data_values = build_data_values(populations, org_unit_map, query.data_element_uid)
    result = import_data_values(client, data_values)

    create_markdown_artifact(key="dhis2-worldbank-import", markdown=result.markdown)
    return result


if __name__ == "__main__":
    load_dotenv()
    dhis2_worldbank_import_flow()
