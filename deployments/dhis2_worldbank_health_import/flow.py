"""DHIS2 World Bank Health Indicators Import.

Fetches 10 health-related indicators from the World Bank API, ensures
the target data elements and data set exist in DHIS2, and imports the
indicator data as data values.

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

DATA_SET_UID = "PfWbHlthDS1"


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class IndicatorConfig(BaseModel):
    """Configuration for a single World Bank health indicator."""

    wb_code: str = Field(description="World Bank indicator code")
    de_uid: str = Field(description="DHIS2 data element UID")
    name: str = Field(description="Data element display name")
    shortName: str = Field(description="Data element short name")


INDICATORS: list[IndicatorConfig] = [
    IndicatorConfig(
        wb_code="SH.DYN.MORT", de_uid="PfWbU5Mort1", name="PR - Under-5 Mortality Rate", shortName="PR - U5 Mortality"
    ),
    IndicatorConfig(
        wb_code="SP.DYN.IMRT.IN",
        de_uid="PfWbInfMrt1",
        name="PR - Infant Mortality Rate",
        shortName="PR - Inf Mortality",
    ),
    IndicatorConfig(
        wb_code="SH.STA.MMRT",
        de_uid="PfWbMatMrt1",
        name="PR - Maternal Mortality Ratio",
        shortName="PR - Mat Mortality",
    ),
    IndicatorConfig(
        wb_code="SP.DYN.LE00.IN", de_uid="PfWbLifExp1", name="PR - Life Expectancy", shortName="PR - Life Expect"
    ),
    IndicatorConfig(
        wb_code="SH.XPD.CHEX.GD.ZS",
        de_uid="PfWbHlthEx1",
        name="PR - Health Expenditure % GDP",
        shortName="PR - Hlth Expend",
    ),
    IndicatorConfig(
        wb_code="SH.TBS.INCD", de_uid="PfWbTbIncd1", name="PR - TB Incidence", shortName="PR - TB Incidence"
    ),
    IndicatorConfig(
        wb_code="SH.IMM.MEAS", de_uid="PfWbMeasIm1", name="PR - Measles Immunization", shortName="PR - Measles Imm"
    ),
    IndicatorConfig(
        wb_code="SH.STA.STNT.ZS", de_uid="PfWbStntPr1", name="PR - Stunting Prevalence", shortName="PR - Stunting"
    ),
    IndicatorConfig(
        wb_code="SP.DYN.TFRT.IN", de_uid="PfWbFertRt1", name="PR - Fertility Rate", shortName="PR - Fertility"
    ),
    IndicatorConfig(
        wb_code="NY.GDP.PCAP.CD", de_uid="PfWbGdpPCp1", name="PR - GDP Per Capita", shortName="PR - GDP PCap"
    ),
]


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


class HealthQuery(BaseModel):
    """Parameters for a World Bank health indicators report."""

    iso3_codes: list[str] = Field(description="ISO3 country codes to fetch")
    start_year: int = Field(description="First year of data range (inclusive)")
    end_year: int = Field(description="Last year of data range (inclusive)")


class IndicatorValue(BaseModel):
    """A single indicator data point from the World Bank."""

    indicator: str = Field(description="World Bank indicator code")
    de_uid: str = Field(description="DHIS2 data element UID")
    year: int = Field(description="Data year")
    value: float = Field(description="Indicator value")


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
    """Ensure the health indicator data elements and data set exist in DHIS2.

    Fetches the level-1 org unit, creates the 10 DEs and DS if needed, and
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

    data_elements = [
        Dhis2DataElement(
            id=ind.de_uid,
            name=ind.name,
            shortName=ind.shortName,
        )
        for ind in INDICATORS
    ]

    data_set = Dhis2DataSet(
        id=DATA_SET_UID,
        name="PR - World Bank Health Indicators",
        shortName="PR - WB Health",
        periodType="Yearly",
        dataSetElements=[Dhis2DataSetElement(dataElement=Dhis2Ref(id=ind.de_uid)) for ind in INDICATORS],
        organisationUnits=[Dhis2Ref(id=org_unit.id)],
    )

    payload: dict[str, Any] = {
        "dataElements": [de.model_dump() for de in data_elements],
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
def fetch_wb_data(
    indicator: IndicatorConfig,
    iso3_codes: list[str],
    start_year: int,
    end_year: int,
) -> list[IndicatorValue]:
    """Fetch a single World Bank indicator for multiple countries.

    Args:
        indicator: Indicator configuration with WB code and DE UID.
        iso3_codes: List of ISO3 country codes.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        List of IndicatorValue entries (null values filtered out).
    """
    codes = ";".join(iso3_codes)
    url = f"{WORLDBANK_API_URL}/country/{codes}/indicator/{indicator.wb_code}"
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
        print(f"No data returned for {indicator.wb_code} / {codes} ({start_year}-{end_year})")
        return []

    results: list[IndicatorValue] = []
    for entry in payload[1]:
        value = entry.get("value")
        if value is None:
            continue
        results.append(
            IndicatorValue(
                indicator=indicator.wb_code,
                de_uid=indicator.de_uid,
                year=int(entry.get("date", 0)),
                value=float(value),
            )
        )

    print(f"Fetched {indicator.wb_code}: {len(results)} records")
    return results


@task
def build_data_values(
    org_unit: OrgUnit,
    all_values: list[IndicatorValue],
) -> list[DataValue]:
    """Transform indicator records into DHIS2 data values.

    Args:
        org_unit: Target organisation unit.
        all_values: Fetched indicator records across all indicators.

    Returns:
        List of DataValue objects ready for import.
    """
    values = [
        DataValue(
            dataElement=iv.de_uid,
            period=str(iv.year),
            orgUnit=org_unit.id,
            value=str(iv.value),
        )
        for iv in all_values
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
        "## DHIS2 World Bank Health Indicators Import",
        "",
        f"**DHIS2 target:** {dhis2_url}",
        f"**Org unit:** {org_unit.name} (`{org_unit.id}`)",
        f"**Data set:** `{DATA_SET_UID}`",
        "",
        "### Import Summary",
        "",
        "| Imported | Updated | Ignored | Total |",
        "|---------|---------|---------|-------|",
        f"| {imported} | {updated} | {ignored} | {len(data_values)} |",
        "",
        "### Data Values",
        "",
        "| Data Element | Period | Value |",
        "|-------------|--------|-------|",
    ]
    for dv in sorted(data_values, key=lambda d: (d.dataElement, d.period)):
        lines.append(f"| {dv.dataElement} | {dv.period} | {dv.value} |")
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


@flow(name="dhis2_worldbank_health_import", log_prints=True)
def dhis2_worldbank_health_import_flow(
    query: HealthQuery | None = None,
) -> ImportResult:
    """Fetch World Bank health indicator data and import into DHIS2.

    Args:
        query: Query parameters. Uses demo defaults if not provided.

    Returns:
        ImportResult with counts and markdown summary.
    """
    if query is None:
        query = HealthQuery(
            iso3_codes=["LAO"],
            start_year=2020,
            end_year=2023,
        )

    creds = get_dhis2_credentials()
    print(f"DHIS2 target: {creds.base_url}")
    client = creds.get_client()

    org_unit = ensure_dhis2_metadata(client)

    all_values: list[IndicatorValue] = []
    for indicator in INDICATORS:
        values = fetch_wb_data(indicator, query.iso3_codes, query.start_year, query.end_year)
        all_values.extend(values)

    data_values = build_data_values(org_unit, all_values)
    result = import_to_dhis2(client, creds.base_url, org_unit, data_values)

    create_markdown_artifact(key="dhis2-worldbank-health-import", markdown=result.markdown)
    return result


if __name__ == "__main__":
    load_dotenv()
    dhis2_worldbank_health_import_flow()
