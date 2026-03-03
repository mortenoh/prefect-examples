"""DHIS2 Open-Meteo Air Quality Import.

Fetches point-based air quality data from the Open-Meteo Air Quality API
for each org unit centroid and imports daily values into DHIS2. Seven
air quality variables are produced:

1. **PM2.5** (ug/m3) -- daily mean fine particulate matter
2. **PM10** (ug/m3) -- daily mean coarse particulate matter
3. **Ozone** (ug/m3) -- daily mean ground-level ozone
4. **Nitrogen dioxide** (ug/m3) -- daily mean NO2
5. **Sulphur dioxide** (ug/m3) -- daily mean SO2
6. **Carbon monoxide** (ug/m3) -- daily mean CO
7. **European AQI** (index) -- daily max European Air Quality Index

No API key required. The API provides current conditions plus a ~5-day
forecast. Each run imports the available window; running regularly
builds a historical record in DHIS2.
"""

from __future__ import annotations

import logging

from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect_climate import ClimateQuery, ImportResult, centroid
from prefect_climate.openmeteo import DailyAirQuality, fetch_daily_air_quality
from prefect_dhis2 import (
    DataValue,
    Dhis2Client,
    Dhis2DataElement,
    Dhis2DataSet,
    Dhis2DataSetElement,
    Dhis2DataValueSet,
    Dhis2MetadataPayload,
    Dhis2Ref,
    OrgUnitGeo,
    get_dhis2_credentials,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PM2_5_DE_UID = "OmAqPm2Est1"
PM10_DE_UID = "OmAqP10Est1"
OZONE_DE_UID = "OmAqO3xEst1"
NO2_DE_UID = "OmAqNo2Est1"
SO2_DE_UID = "OmAqSo2Est1"
CO_DE_UID = "OmAqCoxEst1"
AQI_DE_UID = "OmAqAqiEst1"

DATA_SET_UID = "OmAqDlySet1"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(task_run_name="ensure-dhis2-metadata")
def ensure_dhis2_metadata(
    client: Dhis2Client,
    org_unit_level: int,
) -> list[OrgUnitGeo]:
    """Ensure data elements and data set exist; return org units with geometry."""
    raw_ous = client.fetch_metadata(
        "organisationUnits",
        fields="id,name,geometry",
        filters=[f"level:eq:{org_unit_level}"],
    )
    if not raw_ous:
        msg = f"No level-{org_unit_level} organisation units found in DHIS2"
        raise ValueError(msg)

    org_units = [
        OrgUnitGeo(id=ou["id"], name=ou.get("name", ""), geometry=ou["geometry"])
        for ou in raw_ous
        if ou.get("geometry", {}).get("type") in ("Point", "Polygon", "MultiPolygon")
    ]
    print(f"Found {len(org_units)} level-{org_unit_level} org units with geometry")

    if not org_units:
        msg = f"No level-{org_unit_level} org units with geometry"
        raise ValueError(msg)

    payload = Dhis2MetadataPayload(
        dataElements=[
            Dhis2DataElement(
                id=PM2_5_DE_UID,
                name="PR: OM: AQ: PM2.5",
                shortName="PR: OM: AQ: PM2.5",
                description=(
                    "Daily mean fine particulate matter (PM2.5) from Open-Meteo."
                    " Unit: micrograms per cubic metre (ug/m3)."
                ),
            ),
            Dhis2DataElement(
                id=PM10_DE_UID,
                name="PR: OM: AQ: PM10",
                shortName="PR: OM: AQ: PM10",
                description=(
                    "Daily mean coarse particulate matter (PM10) from Open-Meteo."
                    " Unit: micrograms per cubic metre (ug/m3)."
                ),
            ),
            Dhis2DataElement(
                id=OZONE_DE_UID,
                name="PR: OM: AQ: Ozone",
                shortName="PR: OM: AQ: Ozone",
                description="Daily mean ground-level ozone from Open-Meteo. Unit: micrograms per cubic metre (ug/m3).",
            ),
            Dhis2DataElement(
                id=NO2_DE_UID,
                name="PR: OM: AQ: Nitrogen Dioxide",
                shortName="PR: OM: AQ: NO2",
                description="Daily mean nitrogen dioxide from Open-Meteo. Unit: micrograms per cubic metre (ug/m3).",
            ),
            Dhis2DataElement(
                id=SO2_DE_UID,
                name="PR: OM: AQ: Sulphur Dioxide",
                shortName="PR: OM: AQ: SO2",
                description="Daily mean sulphur dioxide from Open-Meteo. Unit: micrograms per cubic metre (ug/m3).",
            ),
            Dhis2DataElement(
                id=CO_DE_UID,
                name="PR: OM: AQ: Carbon Monoxide",
                shortName="PR: OM: AQ: CO",
                description="Daily mean carbon monoxide from Open-Meteo. Unit: micrograms per cubic metre (ug/m3).",
            ),
            Dhis2DataElement(
                id=AQI_DE_UID,
                name="PR: OM: AQ: European AQI",
                shortName="PR: OM: AQ: EU AQI",
                description="Daily maximum European Air Quality Index from Open-Meteo. Dimensionless index (0-500+).",
            ),
        ],
        dataSets=[
            Dhis2DataSet(
                id=DATA_SET_UID,
                name="PR: OM: AQ: Air Quality",
                shortName="PR: OM: AQ",
                description=(
                    "Open-Meteo air quality daily indicators:"
                    " PM2.5, PM10, ozone, nitrogen dioxide,"
                    " sulphur dioxide, carbon monoxide, and European AQI."
                ),
                periodType="Daily",
                openFuturePeriods=6,
                dataSetElements=[
                    Dhis2DataSetElement(dataElement=Dhis2Ref(id=PM2_5_DE_UID)),
                    Dhis2DataSetElement(dataElement=Dhis2Ref(id=PM10_DE_UID)),
                    Dhis2DataSetElement(dataElement=Dhis2Ref(id=OZONE_DE_UID)),
                    Dhis2DataSetElement(dataElement=Dhis2Ref(id=NO2_DE_UID)),
                    Dhis2DataSetElement(dataElement=Dhis2Ref(id=SO2_DE_UID)),
                    Dhis2DataSetElement(dataElement=Dhis2Ref(id=CO_DE_UID)),
                    Dhis2DataSetElement(dataElement=Dhis2Ref(id=AQI_DE_UID)),
                ],
                organisationUnits=[Dhis2Ref(id=ou.id) for ou in org_units],
            ),
        ],
    )

    result = client.post_metadata(payload.model_dump())
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

    return org_units


@task(
    task_run_name="fetch-air-quality-{org_unit.name}",
    retries=2,
    retry_delay_seconds=[5, 15],
)
def fetch_org_unit_air_quality(
    org_unit: OrgUnitGeo,
) -> tuple[OrgUnitGeo, list[DailyAirQuality]]:
    """Compute centroid of an org unit and fetch its air quality data."""
    lat, lon = centroid(org_unit.geometry)
    print(f"{org_unit.name}: fetching air quality for centroid ({lat:.4f}, {lon:.4f})")
    air_quality = fetch_daily_air_quality(lat, lon)
    print(f"{org_unit.name}: {len(air_quality)} air quality days")
    return (org_unit, air_quality)


@task(task_run_name="build-data-values")
def build_data_values(
    aq_results: list[tuple[OrgUnitGeo, list[DailyAirQuality]]],
) -> Dhis2DataValueSet:
    """Build DHIS2 data values from air quality results."""
    de_map: dict[str, str] = {
        "pm2_5": PM2_5_DE_UID,
        "pm10": PM10_DE_UID,
        "ozone": OZONE_DE_UID,
        "nitrogen_dioxide": NO2_DE_UID,
        "sulphur_dioxide": SO2_DE_UID,
        "carbon_monoxide": CO_DE_UID,
        "european_aqi": AQI_DE_UID,
    }

    values: list[DataValue] = []
    for ou, aq_days in aq_results:
        for day in aq_days:
            period = day.date.replace("-", "")
            for field_name, de_uid in de_map.items():
                val = getattr(day, field_name)
                if val is not None:
                    values.append(
                        DataValue(
                            dataElement=de_uid,
                            period=period,
                            orgUnit=ou.id,
                            categoryOptionCombo="",
                            value=str(val),
                        )
                    )
    print(f"Built {len(values)} data values for {len(aq_results)} org units")
    return Dhis2DataValueSet(dataSet=DATA_SET_UID, dataValues=values)


@task(task_run_name="import-to-dhis2")
def import_to_dhis2(
    client: Dhis2Client,
    dhis2_url: str,
    org_units: list[OrgUnitGeo],
    data_value_set: Dhis2DataValueSet,
) -> ImportResult:
    """POST data values to DHIS2 and return the import summary."""
    if not data_value_set.dataValues:
        print("No data values to import")
        return ImportResult(
            dhis2_url=dhis2_url,
            org_units=org_units,
            markdown="*No data values to import.*",
        )

    result = client.post_data_values(data_value_set.model_dump())

    inner = result.get("response", result)
    counts = inner.get("importCount", {})
    imported = counts.get("imported", 0)
    updated = counts.get("updated", 0)
    ignored = counts.get("ignored", 0)
    status = inner.get("status", result.get("status", "UNKNOWN"))

    data_values = data_value_set.dataValues
    print(f"DHIS2 import: imported={imported}, updated={updated}, ignored={ignored}, status={status}")
    if status not in ("OK", "SUCCESS", "WARNING"):
        print(f"Import response: {result}")

    lines = [
        "## DHIS2 Open-Meteo Air Quality Import",
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
        "| Org Unit | Period | Data Element | Value |",
        "|----------|--------|--------------|-------|",
    ]
    for dv in sorted(data_values, key=lambda d: (d.orgUnit, d.period, d.dataElement)):
        lines.append(f"| {dv.orgUnit} | {dv.period} | {dv.dataElement} | {dv.value} |")
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


@flow(name="dhis2_openmeteo_air_quality_import", log_prints=True)
def dhis2_openmeteo_air_quality_import_flow(
    query: ClimateQuery | None = None,
) -> ImportResult:
    """Fetch Open-Meteo air quality data and import daily values into DHIS2.

    Computes the centroid of each org unit polygon, queries the Open-Meteo
    Air Quality API for that point, aggregates hourly data to daily values,
    and writes 7 air quality indicators into a DHIS2 daily data set.

    The ``query.year`` and ``query.months`` parameters are not used --
    the API provides recent conditions plus a ~5-day forecast.

    Args:
        query: Query parameters (org_unit_level is used; year/months ignored).

    Returns:
        ImportResult with counts and markdown summary.
    """
    if query is None:
        query = ClimateQuery(org_unit_level=2)
        print(f"No query provided, using default: org_unit_level={query.org_unit_level}")

    creds = get_dhis2_credentials()
    print(f"DHIS2 target: {creds.base_url}")
    client = creds.get_client()

    org_units = ensure_dhis2_metadata(client, query.org_unit_level)

    aq_results: list[tuple[OrgUnitGeo, list[DailyAirQuality]]] = []
    for ou in org_units:
        ou_aq = fetch_org_unit_air_quality(ou)
        aq_results.append(ou_aq)

    data_value_set = build_data_values(aq_results)
    import_result = import_to_dhis2(client, creds.base_url, org_units, data_value_set)

    create_markdown_artifact(key="dhis2-openmeteo-air-quality-import", markdown=import_result.markdown)
    return import_result


if __name__ == "__main__":
    load_dotenv()
    dhis2_openmeteo_air_quality_import_flow()
