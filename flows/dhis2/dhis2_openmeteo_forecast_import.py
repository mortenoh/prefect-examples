"""DHIS2 Open-Meteo Weather Forecast Import.

Fetches point-based weather forecasts from the Open-Meteo Forecast API
for each org unit centroid and imports daily values into DHIS2. Six
weather variables are produced:

1. **Temperature** (Celsius) -- daily mean ``temperature_2m``
2. **Precipitation** (mm) -- daily sum ``precipitation``
3. **Relative humidity** (%) -- daily mean ``relative_humidity_2m``
4. **Wind speed** (m/s) -- daily mean ``wind_speed_10m``
5. **Cloud cover** (%) -- daily mean ``cloud_cover``
6. **Air pressure** (hPa) -- daily mean ``pressure_msl``

No API key required. The API provides up to 16 days of forecast data.
Each run imports the current forecast window; running regularly builds
a historical record in DHIS2.
"""

from __future__ import annotations

import logging

from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect_climate import ClimateQuery, ImportResult, centroid
from prefect_climate.openmeteo import DailyWeather, fetch_daily_forecast
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

TEMPERATURE_DE_UID = "OmFcTmpEst1"
PRECIPITATION_DE_UID = "OmFcPrcEst1"
HUMIDITY_DE_UID = "OmFcHumEst1"
WIND_SPEED_DE_UID = "OmFcWndEst1"
CLOUD_COVER_DE_UID = "OmFcCldEst1"
PRESSURE_DE_UID = "OmFcPrsEst1"

DATA_SET_UID = "OmFcDlySet1"


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
                name="PR: OM: WF: Temperature",
                shortName="PR: OM: WF: Temp",
                description="Daily mean air temperature from Open-Meteo forecast. Unit: degrees Celsius (C).",
            ),
            Dhis2DataElement(
                id=PRECIPITATION_DE_UID,
                name="PR: OM: WF: Precipitation",
                shortName="PR: OM: WF: Precip",
                description="Daily total precipitation from Open-Meteo forecast. Unit: millimetres (mm).",
            ),
            Dhis2DataElement(
                id=HUMIDITY_DE_UID,
                name="PR: OM: WF: Relative Humidity",
                shortName="PR: OM: WF: Humidity",
                description="Daily mean relative humidity from Open-Meteo forecast. Unit: percent (%).",
            ),
            Dhis2DataElement(
                id=WIND_SPEED_DE_UID,
                name="PR: OM: WF: Wind Speed",
                shortName="PR: OM: WF: Wind",
                description="Daily mean wind speed from Open-Meteo forecast. Unit: metres per second (m/s).",
            ),
            Dhis2DataElement(
                id=CLOUD_COVER_DE_UID,
                name="PR: OM: WF: Cloud Cover",
                shortName="PR: OM: WF: Cloud",
                description="Daily mean cloud cover from Open-Meteo forecast. Unit: percent (%).",
            ),
            Dhis2DataElement(
                id=PRESSURE_DE_UID,
                name="PR: OM: WF: Air Pressure",
                shortName="PR: OM: WF: Pressure",
                description="Daily mean sea-level air pressure from Open-Meteo forecast. Unit: hectopascals (hPa).",
            ),
        ],
        dataSets=[
            Dhis2DataSet(
                id=DATA_SET_UID,
                name="PR: OM: WF: Weather Forecast",
                shortName="PR: OM: WF: Forecast",
                description=(
                    "Open-Meteo weather forecast daily indicators:"
                    " temperature, precipitation, humidity, wind speed,"
                    " cloud cover, and air pressure."
                ),
                periodType="Daily",
                openFuturePeriods=17,
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
    task_run_name="fetch-forecast-{org_unit.name}",
    retries=2,
    retry_delay_seconds=[5, 15],
)
def fetch_org_unit_forecast(
    org_unit: OrgUnitGeo,
) -> tuple[OrgUnitGeo, list[DailyWeather]]:
    """Compute centroid of an org unit and fetch its weather forecast."""
    lat, lon = centroid(org_unit.geometry)
    print(f"{org_unit.name}: fetching forecast for centroid ({lat:.4f}, {lon:.4f})")
    forecasts = fetch_daily_forecast(lat, lon)
    print(f"{org_unit.name}: {len(forecasts)} forecast days")
    return (org_unit, forecasts)


@task(task_run_name="build-data-values")
def build_data_values(
    forecast_results: list[tuple[OrgUnitGeo, list[DailyWeather]]],
) -> Dhis2DataValueSet:
    """Build DHIS2 data values from forecast results."""
    de_map: dict[str, str] = {
        "temperature": TEMPERATURE_DE_UID,
        "precipitation": PRECIPITATION_DE_UID,
        "humidity": HUMIDITY_DE_UID,
        "wind_speed": WIND_SPEED_DE_UID,
        "cloud_cover": CLOUD_COVER_DE_UID,
        "pressure": PRESSURE_DE_UID,
    }

    values: list[DataValue] = []
    for ou, forecasts in forecast_results:
        for fc in forecasts:
            period = fc.date.replace("-", "")
            for field_name, de_uid in de_map.items():
                val = getattr(fc, field_name)
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
    print(f"Built {len(values)} data values for {len(forecast_results)} org units")
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
        "## DHIS2 Open-Meteo Weather Forecast Import",
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


@flow(name="dhis2_openmeteo_forecast_import", log_prints=True)
def dhis2_openmeteo_forecast_import_flow(
    query: ClimateQuery | None = None,
) -> ImportResult:
    """Fetch Open-Meteo weather forecasts and import daily values into DHIS2.

    Computes the centroid of each org unit polygon, queries the Open-Meteo
    Forecast API for that point, aggregates hourly data to daily values,
    and writes 6 weather indicators into a DHIS2 daily data set.

    The ``query.year`` and ``query.months`` parameters are not used --
    the forecast window is always the current time plus ~16 days.

    Args:
        query: Query parameters (org_unit_level is used; year/months ignored).

    Returns:
        ImportResult with counts and markdown summary.
    """
    if query is None:
        query = ClimateQuery(org_unit_level=4)
        print(f"No query provided, using default: org_unit_level={query.org_unit_level}")

    creds = get_dhis2_credentials()
    print(f"DHIS2 target: {creds.base_url}")
    client = creds.get_client()

    org_units = ensure_dhis2_metadata(client, query.org_unit_level)

    forecast_results: list[tuple[OrgUnitGeo, list[DailyWeather]]] = []
    for ou in org_units:
        ou_forecast = fetch_org_unit_forecast(ou)
        forecast_results.append(ou_forecast)

    data_value_set = build_data_values(forecast_results)
    import_result = import_to_dhis2(client, creds.base_url, org_units, data_value_set)

    create_markdown_artifact(key="dhis2-openmeteo-forecast-import", markdown=import_result.markdown)
    return import_result


if __name__ == "__main__":
    load_dotenv()
    dhis2_openmeteo_forecast_import_flow()
