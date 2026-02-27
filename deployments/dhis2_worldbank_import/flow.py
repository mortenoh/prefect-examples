"""DHIS2 World Bank Population Import.

Fetches total population (SP.POP.TOTL) from the World Bank API, ensures
the target data element and data set exist in DHIS2, and imports the
population data as data values.

Airflow equivalent: PythonOperator chain with World Bank fetch + DHIS2 setup.
Prefect approach:   typed models, retry-enabled tasks, markdown artifact.
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


class OrgUnit(BaseModel):
    """A DHIS2 organisation unit."""

    id: str = Field(description="DHIS2 UID")
    name: str = Field(default="", description="Display name")


class DataValue(BaseModel):
    """A single DHIS2 data value."""

    dataElement: str = Field(description="Data element UID")
    period: str = Field(description="Period (e.g. '2023' for Yearly)")
    orgUnit: str = Field(description="Organisation unit UID")
    value: str = Field(description="Value as string")


class ImportResult(BaseModel):
    """Summary of a DHIS2 data value import."""

    dhis2_url: str = Field(description="Target DHIS2 instance URL")
    org_unit: OrgUnit = Field(description="Level 1 org unit used as target")
    imported: int = Field(default=0, description="Records imported")
    updated: int = Field(default=0, description="Records updated")
    ignored: int = Field(default=0, description="Records ignored")
    total: int = Field(default=0, description="Total records sent")
    markdown: str = Field(default="", description="Markdown summary")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def ensure_dhis2_metadata(client: Dhis2Client) -> OrgUnit:
    """Ensure the population data element and data set exist in DHIS2.

    Fetches the level-1 org unit, creates the DE and DS if needed, and
    returns the org unit to use as the data target.

    Args:
        client: Authenticated DHIS2 client.

    Returns:
        Level-1 OrgUnit.
    """
    level1_ous = client.fetch_metadata("organisationUnits", fields="id,name", filters=["level:eq:1"])
    if not level1_ous:
        msg = "No level-1 organisation unit found in DHIS2"
        raise ValueError(msg)

    org_unit = OrgUnit(id=level1_ous[0]["id"], name=level1_ous[0].get("name", ""))
    print(f"Level 1 org unit: {org_unit.name} ({org_unit.id})")

    data_element = Dhis2DataElement(
        id=DATA_ELEMENT_UID,
        name="Prefect - Population",
        shortName="PR - Population",
    )
    data_set = Dhis2DataSet(
        id=DATA_SET_UID,
        name="Prefect - Population",
        shortName="PR - Population",
        periodType="Yearly",
        dataSetElements=[Dhis2DataSetElement(dataElement=Dhis2Ref(id=DATA_ELEMENT_UID))],
        organisationUnits=[Dhis2Ref(id=org_unit.id)],
    )

    payload: dict[str, Any] = {
        "dataElements": [data_element.model_dump()],
        "dataSets": [data_set.model_dump()],
    }

    result = client.post_metadata(payload)
    stats = result.get("stats", {})
    status = result.get("status", "UNKNOWN")
    print(
        f"Metadata sync: status={status}, "
        f"created={stats.get('created', 0)}, "
        f"updated={stats.get('updated', 0)}, "
        f"ignored={stats.get('ignored', 0)}"
    )
    if status not in ("OK", "WARNING"):
        print(f"Metadata response: {result}")

    return org_unit


@task(retries=2, retry_delay_seconds=[2, 5])
def fetch_population_data(
    iso3_codes: list[str],
    start_year: int,
    end_year: int,
) -> list[CountryPopulation]:
    """Fetch total population from the World Bank for multiple countries.

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
def build_data_values(
    org_unit: OrgUnit,
    populations: list[CountryPopulation],
) -> list[DataValue]:
    """Transform population records into DHIS2 data values.

    Args:
        org_unit: Target organisation unit.
        populations: Fetched population records.

    Returns:
        List of DataValue objects ready for import.
    """
    values = [
        DataValue(
            dataElement=DATA_ELEMENT_UID,
            period=str(pop.year),
            orgUnit=org_unit.id,
            value=str(pop.population),
        )
        for pop in populations
    ]
    print(f"Built {len(values)} data values for org unit {org_unit.name} ({org_unit.id})")
    return values


@task
def import_to_dhis2(
    client: Dhis2Client,
    dhis2_url: str,
    org_unit: OrgUnit,
    data_values: list[DataValue],
) -> ImportResult:
    """POST data values to DHIS2 and return the import summary.

    Args:
        client: Authenticated DHIS2 client.
        dhis2_url: DHIS2 instance base URL.
        org_unit: Target organisation unit.
        data_values: Data values to import.

    Returns:
        ImportResult with counts and markdown summary.
    """
    if not data_values:
        print("No data values to import")
        return ImportResult(
            dhis2_url=dhis2_url,
            org_unit=org_unit,
            markdown="*No data values to import.*",
        )

    payload = {"dataValues": [dv.model_dump() for dv in data_values]}
    result = client.post_data_values(payload)

    counts = result.get("importCount", {})
    imported = counts.get("imported", 0)
    updated = counts.get("updated", 0)
    ignored = counts.get("ignored", 0)

    print(
        f"DHIS2 import: imported={imported}, updated={updated}, "
        f"ignored={ignored}, status={result.get('status', 'UNKNOWN')}"
    )
    if result.get("status") not in ("SUCCESS", "WARNING"):
        print(f"Import response: {result}")

    lines = [
        "## DHIS2 World Bank Population Import",
        "",
        f"**DHIS2 target:** {dhis2_url}",
        f"**Org unit:** {org_unit.name} (`{org_unit.id}`)",
        f"**Data element:** `{DATA_ELEMENT_UID}` | **Data set:** `{DATA_SET_UID}`",
        "",
        "### Import Summary",
        "",
        "| Imported | Updated | Ignored | Total |",
        "|---------|---------|---------|-------|",
        f"| {imported} | {updated} | {ignored} | {len(data_values)} |",
        "",
        "### Data Values",
        "",
        "| Period | Value |",
        "|--------|-------|",
    ]
    for dv in sorted(data_values, key=lambda d: d.period):
        lines.append(f"| {dv.period} | {int(dv.value):,} |")
    lines.append("")

    return ImportResult(
        dhis2_url=dhis2_url,
        org_unit=org_unit,
        imported=imported,
        updated=updated,
        ignored=ignored,
        total=len(data_values),
        markdown="\n".join(lines),
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="dhis2_worldbank_import", log_prints=True)
def dhis2_worldbank_import_flow(
    query: PopulationQuery | None = None,
) -> ImportResult:
    """Fetch World Bank population data and import into DHIS2.

    Args:
        query: Query parameters. Uses demo defaults if not provided.

    Returns:
        ImportResult with counts and markdown summary.
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

    org_unit = ensure_dhis2_metadata(client)
    populations = fetch_population_data(query.iso3_codes, query.start_year, query.end_year)
    data_values = build_data_values(org_unit, populations)
    result = import_to_dhis2(client, creds.base_url, org_unit, data_values)

    create_markdown_artifact(key="dhis2-worldbank-import", markdown=result.markdown)
    return result


if __name__ == "__main__":
    load_dotenv()
    dhis2_worldbank_import_flow()
