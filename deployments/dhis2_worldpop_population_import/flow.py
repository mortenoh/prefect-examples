"""DHIS2 WorldPop Population Import.

Reads org unit boundaries (GeoJSON polygons) from DHIS2, queries the WorldPop
age-sex API for gridded population estimates, and writes sex-disaggregated
results back to DHIS2 using category combinations.

Airflow equivalent: PythonOperator chain with geometry fetch + WorldPop query + DHIS2 POST.
Prefect approach:   typed models, retry-enabled tasks, category combo resolution, markdown artifact.
"""

from __future__ import annotations

import json
import logging
import time
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

WORLDPOP_STATS_URL = "https://api.worldpop.org/v1/services/stats"
POLL_INTERVAL_SECONDS = 10
POLL_TIMEOUT_SECONDS = 600

DATA_ELEMENT_UID = "PfWpPopEst1"
DATA_SET_UID = "PfWpPopSet1"
CAT_OPTION_MALE_UID = "PfWpSexMal1"
CAT_OPTION_FEMALE_UID = "PfWpSexFem1"
CATEGORY_UID = "PfWpSexCat1"
CAT_COMBO_UID = "PfWpSexCCo1"


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class Dhis2Ref(BaseModel):
    """A DHIS2 object reference (just an id)."""

    id: str = Field(description="DHIS2 UID")


class Dhis2CategoryOption(BaseModel):
    """A DHIS2 category option for the metadata API."""

    id: str
    name: str
    shortName: str


class Dhis2Category(BaseModel):
    """A DHIS2 category for the metadata API."""

    id: str
    name: str
    shortName: str
    dataDimensionType: str = "DISAGGREGATION"
    categoryOptions: list[Dhis2Ref]


class Dhis2CategoryCombo(BaseModel):
    """A DHIS2 category combination for the metadata API."""

    id: str
    name: str
    dataDimensionType: str = "DISAGGREGATION"
    categories: list[Dhis2Ref]


class Dhis2DataElement(BaseModel):
    """A DHIS2 data element for the metadata API."""

    id: str = Field(description="Fixed UID")
    name: str = Field(description="Display name")
    shortName: str = Field(description="Short name")
    domainType: str = Field(default="AGGREGATE", description="AGGREGATE or TRACKER")
    valueType: str = Field(default="NUMBER", description="Value type")
    aggregationType: str = Field(default="SUM", description="Aggregation type")
    categoryCombo: Dhis2Ref | None = None


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


class OrgUnitGeo(BaseModel):
    """A DHIS2 organisation unit with geometry."""

    id: str
    name: str
    geometry: dict[str, Any]


class WorldPopResult(BaseModel):
    """Population result for a single org unit."""

    org_unit_id: str
    org_unit_name: str
    male: float
    female: float


class ImportQuery(BaseModel):
    """Parameters for a WorldPop population import."""

    year: int = Field(default=2020, ge=2000, le=2020)


class CocMapping(BaseModel):
    """categoryOptionCombo UIDs resolved from DHIS2."""

    male: str
    female: str


class DataValue(BaseModel):
    """A single DHIS2 data value with category option combo."""

    dataElement: str = Field(description="Data element UID")
    period: str = Field(description="Period (e.g. '2020' for Yearly)")
    orgUnit: str = Field(description="Organisation unit UID")
    categoryOptionCombo: str = Field(description="Category option combo UID")
    value: str = Field(description="Value as string")


class ImportResult(BaseModel):
    """Summary of a DHIS2 data value import."""

    dhis2_url: str = Field(description="Target DHIS2 instance URL")
    org_units: list[OrgUnitGeo] = Field(description="Org units with geometry")
    imported: int = Field(default=0, description="Records imported")
    updated: int = Field(default=0, description="Records updated")
    ignored: int = Field(default=0, description="Records ignored")
    total: int = Field(default=0, description="Total records sent")
    markdown: str = Field(default="", description="Markdown summary")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def ensure_dhis2_metadata(client: Dhis2Client) -> tuple[list[OrgUnitGeo], CocMapping]:
    """Ensure category options, category, category combo, DE, and DS exist.

    Fetches level-1 org units with geometry, posts all metadata, then
    resolves the auto-generated categoryOptionCombo UIDs.

    Args:
        client: Authenticated DHIS2 client.

    Returns:
        Tuple of (org units with polygon geometry, COC mapping).
    """
    raw_ous = client.fetch_metadata(
        "organisationUnits",
        fields="id,name,geometry",
        filters=["level:eq:1"],
    )
    if not raw_ous:
        msg = "No level-1 organisation unit found in DHIS2"
        raise ValueError(msg)

    org_units = [
        OrgUnitGeo(id=ou["id"], name=ou.get("name", ""), geometry=ou["geometry"])
        for ou in raw_ous
        if ou.get("geometry", {}).get("type") in ("Polygon", "MultiPolygon")
    ]
    print(f"Found {len(org_units)} level-1 org units with polygon geometry")

    if not org_units:
        msg = "No level-1 org units with Polygon/MultiPolygon geometry"
        raise ValueError(msg)

    # Reuse existing Male/Female category options if present on the server
    existing_cos = client.fetch_metadata(
        "categoryOptions",
        fields="id,name",
        filters=["name:in:[Male,Female]"],
    )
    male_co_uid = CAT_OPTION_MALE_UID
    female_co_uid = CAT_OPTION_FEMALE_UID
    for co in existing_cos:
        if co["name"] == "Male":
            male_co_uid = co["id"]
        elif co["name"] == "Female":
            female_co_uid = co["id"]

    new_cos = male_co_uid == CAT_OPTION_MALE_UID or female_co_uid == CAT_OPTION_FEMALE_UID
    if not new_cos:
        print(f"Reusing existing category options: Male={male_co_uid}, Female={female_co_uid}")

    payload: dict[str, Any] = {}

    if new_cos:
        cos_to_create = []
        if male_co_uid == CAT_OPTION_MALE_UID:
            cos_to_create.append(
                Dhis2CategoryOption(id=CAT_OPTION_MALE_UID, name="Male", shortName="Male").model_dump()
            )
        if female_co_uid == CAT_OPTION_FEMALE_UID:
            cos_to_create.append(
                Dhis2CategoryOption(id=CAT_OPTION_FEMALE_UID, name="Female", shortName="Female").model_dump()
            )
        if cos_to_create:
            payload["categoryOptions"] = cos_to_create

    payload["categories"] = [
        Dhis2Category(
            id=CATEGORY_UID,
            name="Sex",
            shortName="Sex",
            categoryOptions=[Dhis2Ref(id=male_co_uid), Dhis2Ref(id=female_co_uid)],
        ).model_dump(),
    ]
    payload["categoryCombos"] = [
        Dhis2CategoryCombo(
            id=CAT_COMBO_UID,
            name="Sex",
            categories=[Dhis2Ref(id=CATEGORY_UID)],
        ).model_dump(),
    ]
    payload["dataElements"] = [
        Dhis2DataElement(
            id=DATA_ELEMENT_UID,
            name="PR - WorldPop Population",
            shortName="PR - WP Pop",
            categoryCombo=Dhis2Ref(id=CAT_COMBO_UID),
        ).model_dump(),
    ]
    payload["dataSets"] = [
        Dhis2DataSet(
            id=DATA_SET_UID,
            name="PR - WorldPop Population",
            shortName="PR - WP Pop",
            dataSetElements=[Dhis2DataSetElement(dataElement=Dhis2Ref(id=DATA_ELEMENT_UID))],
            organisationUnits=[Dhis2Ref(id=ou.id) for ou in org_units],
        ).model_dump(),
    ]

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

    # Regenerate categoryOptionCombos after metadata changes
    client.run_maintenance("categoryOptionComboUpdate")
    print("Triggered categoryOptionCombo regeneration")

    # Resolve auto-generated categoryOptionCombos
    cocs = client.fetch_metadata(
        "categoryOptionCombos",
        fields="id,name,categoryOptions[id]",
        filters=["categoryCombo.id:eq:" + CAT_COMBO_UID],
    )

    male_coc_uid = ""
    female_coc_uid = ""
    for coc in cocs:
        option_ids = {co["id"] for co in coc.get("categoryOptions", [])}
        if male_co_uid in option_ids:
            male_coc_uid = coc["id"]
        elif female_co_uid in option_ids:
            female_coc_uid = coc["id"]

    if not male_coc_uid or not female_coc_uid:
        msg = f"Could not resolve COCs for category combo {CAT_COMBO_UID}"
        raise ValueError(msg)

    print(f"Resolved COCs: male={male_coc_uid}, female={female_coc_uid}")
    return org_units, CocMapping(male=male_coc_uid, female=female_coc_uid)


def _poll_async_result(task_id: str, form_data: dict[str, str]) -> dict[str, Any]:
    """Poll the WorldPop async task endpoint until completion or timeout.

    The WorldPop API requires the original request parameters alongside the
    taskid when polling for results.

    Args:
        task_id: WorldPop async task identifier.
        form_data: Original form data (dataset, year, geojson) to re-send.

    Returns:
        Final API response body.
    """
    poll_data = {**form_data, "taskid": task_id}
    start = time.monotonic()
    attempt = 0
    with httpx.Client(timeout=30) as client:
        while time.monotonic() - start < POLL_TIMEOUT_SECONDS:
            attempt += 1
            resp = client.post(WORLDPOP_STATS_URL, data=poll_data)
            resp.raise_for_status()
            body = resp.json()
            status = body.get("status", "")
            elapsed = int(time.monotonic() - start)
            print(f"  Poll #{attempt} ({elapsed}s): status={status}")
            if status == "finished":
                return body  # type: ignore[no-any-return]
            if status == "error":
                msg = f"Async task {task_id} failed"
                raise RuntimeError(msg)
            time.sleep(POLL_INTERVAL_SECONDS)
    msg = f"Async task {task_id} timed out after {POLL_TIMEOUT_SECONDS}s"
    raise TimeoutError(msg)


@task(retries=2, retry_delay_seconds=[2, 5])
def fetch_worldpop_population(org_unit: OrgUnitGeo, year: int) -> WorldPopResult:
    """Query the WorldPop age-sex API for a single org unit polygon.

    Args:
        org_unit: Org unit with Polygon/MultiPolygon geometry.
        year: Population year (2000-2020).

    Returns:
        WorldPopResult with male and female population totals.
    """
    data: dict[str, str] = {
        "dataset": "wpgpas",
        "year": str(year),
        "geojson": json.dumps(org_unit.geometry),
        "runasync": "true",
    }

    with httpx.Client(timeout=30) as client:
        resp = client.post(WORLDPOP_STATS_URL, data=data)
        resp.raise_for_status()
        body = resp.json()

    if body.get("status") == "created":
        task_id = body.get("taskid", "")
        print(f"Async task created for {org_unit.name}: {task_id}, polling...")
        body = _poll_async_result(task_id, data)

    pyramid = body.get("data", {}).get("agesexpyramid", {})

    male_total = sum(float(pyramid.get(f"M_{i}", 0)) for i in range(17))
    female_total = sum(float(pyramid.get(f"F_{i}", 0)) for i in range(17))

    print(f"{org_unit.name}: male={male_total:,.0f}, female={female_total:,.0f}")
    return WorldPopResult(
        org_unit_id=org_unit.id,
        org_unit_name=org_unit.name,
        male=male_total,
        female=female_total,
    )


@task
def build_data_values(
    results: list[WorldPopResult],
    year: int,
    coc_mapping: CocMapping,
) -> list[DataValue]:
    """Build DHIS2 data values from WorldPop results.

    Creates two DataValues per org unit (male + female), each tagged with the
    appropriate categoryOptionCombo UID.

    Args:
        results: WorldPop population results.
        year: Data year for the period.
        coc_mapping: Resolved COC UIDs for male and female.

    Returns:
        List of DataValue objects.
    """
    values: list[DataValue] = []
    for r in results:
        values.append(
            DataValue(
                dataElement=DATA_ELEMENT_UID,
                period=str(year),
                orgUnit=r.org_unit_id,
                categoryOptionCombo=coc_mapping.male,
                value=str(round(r.male)),
            )
        )
        values.append(
            DataValue(
                dataElement=DATA_ELEMENT_UID,
                period=str(year),
                orgUnit=r.org_unit_id,
                categoryOptionCombo=coc_mapping.female,
                value=str(round(r.female)),
            )
        )
    print(f"Built {len(values)} data values for {len(results)} org units")
    return values


@task
def import_to_dhis2(
    client: Dhis2Client,
    dhis2_url: str,
    org_units: list[OrgUnitGeo],
    data_values: list[DataValue],
) -> ImportResult:
    """POST data values to DHIS2 and return the import summary.

    Args:
        client: Authenticated DHIS2 client.
        dhis2_url: DHIS2 instance base URL.
        org_units: Org units included in the import.
        data_values: Data values to import.

    Returns:
        ImportResult with counts and markdown summary.
    """
    if not data_values:
        print("No data values to import")
        return ImportResult(
            dhis2_url=dhis2_url,
            org_units=org_units,
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
        "## DHIS2 WorldPop Population Import",
        "",
        f"**DHIS2 target:** {dhis2_url}",
        f"**Data set:** `{DATA_SET_UID}`",
        f"**Org units:** {len(org_units)}",
        "",
        "### Import Summary",
        "",
        "| Imported | Updated | Ignored | Total |",
        "|---------|---------|---------|-------|",
        f"| {imported} | {updated} | {ignored} | {len(data_values)} |",
        "",
        "### Data Values",
        "",
        "| Org Unit | COC | Period | Value |",
        "|----------|-----|--------|-------|",
    ]
    for dv in sorted(data_values, key=lambda d: (d.orgUnit, d.categoryOptionCombo)):
        lines.append(f"| {dv.orgUnit} | {dv.categoryOptionCombo} | {dv.period} | {dv.value} |")
    lines.append("")

    return ImportResult(
        dhis2_url=dhis2_url,
        org_units=org_units,
        imported=imported,
        updated=updated,
        ignored=ignored,
        total=len(data_values),
        markdown="\n".join(lines),
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="dhis2_worldpop_population_import", log_prints=True)
def dhis2_worldpop_population_import_flow(
    query: ImportQuery | None = None,
) -> ImportResult:
    """Fetch WorldPop population data by sex and import into DHIS2.

    Reads org unit polygon geometry from DHIS2, queries the WorldPop age-sex
    API, and writes male/female population values using category combos.

    Args:
        query: Query parameters. Uses defaults (year=2020) if not provided.

    Returns:
        ImportResult with counts and markdown summary.
    """
    if query is None:
        query = ImportQuery()

    creds = get_dhis2_credentials()
    print(f"DHIS2 target: {creds.base_url}")
    client = creds.get_client()

    org_units, coc_mapping = ensure_dhis2_metadata(client)

    results: list[WorldPopResult] = []
    for ou in org_units:
        wp_result = fetch_worldpop_population(ou, query.year)
        results.append(wp_result)

    data_values = build_data_values(results, query.year, coc_mapping)
    result = import_to_dhis2(client, creds.base_url, org_units, data_values)

    create_markdown_artifact(key="dhis2-worldpop-population-import", markdown=result.markdown)
    return result


if __name__ == "__main__":
    load_dotenv()
    dhis2_worldpop_population_import_flow()
