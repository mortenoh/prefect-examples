"""DHIS2 Open-Meteo Historical Weather Import.

Fetches point-based historical weather data from the Open-Meteo Archive API
for each org unit centroid and imports daily values into DHIS2. Six weather
variables are produced:

1. **Temperature** (Celsius) -- daily mean ``temperature_2m``
2. **Precipitation** (mm) -- daily sum ``precipitation``
3. **Relative humidity** (%) -- daily mean ``relative_humidity_2m``
4. **Wind speed** (m/s) -- daily mean ``wind_speed_10m``
5. **Cloud cover** (%) -- daily mean ``cloud_cover``
6. **Air pressure** (hPa) -- daily mean ``pressure_msl``

No API key required. Historical data is available from 1940 to present.
Uses ``query.year`` and ``query.months`` to select the time window.
"""

from __future__ import annotations

import logging

from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect_climate import ClimateQuery, ImportResult, centroid
from prefect_climate.openmeteo import DailyWeather, fetch_daily_historical
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

TEMPERATURE_DE_UID = "OmHwTmpEst1"
PRECIPITATION_DE_UID = "OmHwPrcEst1"
HUMIDITY_DE_UID = "OmHwHumEst1"
WIND_SPEED_DE_UID = "OmHwWndEst1"
CLOUD_COVER_DE_UID = "OmHwCldEst1"
PRESSURE_DE_UID = "OmHwPrsEst1"

DATA_SET_UID = "OmHwDlySet1"


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
        if ou.get("geometry", {}).get("type") in ("Polygon", "MultiPolygon")
    ]
    print(f"Found {len(org_units)} level-{org_unit_level} org units with polygon geometry")

    if not org_units:
        msg = f"No level-{org_unit_level} org units with Polygon/MultiPolygon geometry"
        raise ValueError(msg)

    payload = Dhis2MetadataPayload(
        dataElements=[
            Dhis2DataElement(
                id=TEMPERATURE_DE_UID,
                name="PR: OM: WH: Temperature",
                shortName="PR: OM: WH: Temp",
                description="Daily mean air temperature from Open-Meteo historical archive. Unit: degrees Celsius (C).",
            ),
            Dhis2DataElement(
                id=PRECIPITATION_DE_UID,
                name="PR: OM: WH: Precipitation",
                shortName="PR: OM: WH: Precip",
                description="Daily total precipitation from Open-Meteo historical archive. Unit: millimetres (mm).",
            ),
            Dhis2DataElement(
                id=HUMIDITY_DE_UID,
                name="PR: OM: WH: Relative Humidity",
                shortName="PR: OM: WH: Rel Humidity",
                description="Daily mean relative humidity from Open-Meteo historical archive. Unit: percent (%).",
            ),
            Dhis2DataElement(
                id=WIND_SPEED_DE_UID,
                name="PR: OM: WH: Wind Speed",
                shortName="PR: OM: WH: Wind Speed",
                description="Daily mean wind speed from Open-Meteo historical archive. Unit: metres per second (m/s).",
            ),
            Dhis2DataElement(
                id=CLOUD_COVER_DE_UID,
                name="PR: OM: WH: Cloud Cover",
                shortName="PR: OM: WH: Cloud Cover",
                description="Daily mean cloud cover from Open-Meteo historical archive. Unit: percent (%).",
            ),
            Dhis2DataElement(
                id=PRESSURE_DE_UID,
                name="PR: OM: WH: Air Pressure",
                shortName="PR: OM: WH: Air Pressure",
                description=(
                    "Daily mean sea-level air pressure from Open-Meteo historical archive."
                    " Unit: hectopascals (hPa)."
                ),
            ),
        ],
        dataSets=[
            Dhis2DataSet(
                id=DATA_SET_UID,
                name="PR: OM: WH: Historical Weather",
                shortName="PR: OM: WH: Historical",
                description=(
                    "Open-Meteo historical daily weather indicators:"
                    " temperature, precipitation, humidity, wind speed,"
                    " cloud cover, and air pressure."
                ),
                periodType="Daily",
                openFuturePeriods=0,
                dataSetElements=[
                    Dhis2DataSetElement(dataElement=Dhis2Ref(id=TEMPERATURE_DE_UID)),
                    Dhis2DataSetElement(dataElement=Dhis2Ref(id=PRECIPITATION_DE_UID)),
                    Dhis2DataSetElement(dataElement=Dhis2Ref(id=HUMIDITY_DE_UID)),
                    Dhis2DataSetElement(dataElement=Dhis2Ref(id=WIND_SPEED_DE_UID)),
                    Dhis2DataSetElement(dataElement=Dhis2Ref(id=CLOUD_COVER_DE_UID)),
                    Dhis2DataSetElement(dataElement=Dhis2Ref(id=PRESSURE_DE_UID)),
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
    task_run_name="fetch-historical-{org_unit.name}",
    retries=2,
    retry_delay_seconds=[5, 15],
)
def fetch_org_unit_historical(
    org_unit: OrgUnitGeo,
    year: int,
    months: list[int],
) -> tuple[OrgUnitGeo, list[DailyWeather]]:
    """Compute centroid of an org unit and fetch its historical weather."""
    lat, lon = centroid(org_unit.geometry)
    print(f"{org_unit.name}: fetching historical weather for centroid ({lat:.4f}, {lon:.4f})")
    weather = fetch_daily_historical(lat, lon, year, months)
    print(f"{org_unit.name}: {len(weather)} historical days")
    return (org_unit, weather)


@task(task_run_name="build-data-values")
def build_data_values(
    weather_results: list[tuple[OrgUnitGeo, list[DailyWeather]]],
) -> Dhis2DataValueSet:
    """Build DHIS2 data values from historical weather results."""
    de_map: dict[str, str] = {
        "temperature": TEMPERATURE_DE_UID,
        "precipitation": PRECIPITATION_DE_UID,
        "humidity": HUMIDITY_DE_UID,
        "wind_speed": WIND_SPEED_DE_UID,
        "cloud_cover": CLOUD_COVER_DE_UID,
        "pressure": PRESSURE_DE_UID,
    }

    values: list[DataValue] = []
    for ou, weather_days in weather_results:
        for day in weather_days:
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
    print(f"Built {len(values)} data values for {len(weather_results)} org units")
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
        "## DHIS2 Open-Meteo Historical Weather Import",
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


@flow(name="dhis2_openmeteo_historical_import", log_prints=True)
def dhis2_openmeteo_historical_import_flow(
    query: ClimateQuery | None = None,
) -> ImportResult:
    """Fetch Open-Meteo historical weather and import daily values into DHIS2.

    Computes the centroid of each org unit polygon, queries the Open-Meteo
    Archive API for that point, aggregates hourly data to daily values,
    and writes 6 weather indicators into a DHIS2 daily data set.

    Uses ``query.year`` and ``query.months`` to select the time window.

    Args:
        query: Query parameters (org_unit_level, year, months).

    Returns:
        ImportResult with counts and markdown summary.
    """
    if query is None:
        query = ClimateQuery(org_unit_level=4)
        print(
            f"No query provided, using default: org_unit_level={query.org_unit_level}, "
            f"year={query.year}, months={query.months}"
        )

    creds = get_dhis2_credentials()
    print(f"DHIS2 target: {creds.base_url}")
    client = creds.get_client()

    org_units = ensure_dhis2_metadata(client, query.org_unit_level)

    weather_results: list[tuple[OrgUnitGeo, list[DailyWeather]]] = []
    for ou in org_units:
        ou_weather = fetch_org_unit_historical(ou, query.year, query.months)
        weather_results.append(ou_weather)

    data_value_set = build_data_values(weather_results)
    import_result = import_to_dhis2(client, creds.base_url, org_units, data_value_set)

    create_markdown_artifact(key="dhis2-openmeteo-historical-import", markdown=import_result.markdown)
    return import_result


if __name__ == "__main__":
    load_dotenv()
    dhis2_openmeteo_historical_import_flow()
