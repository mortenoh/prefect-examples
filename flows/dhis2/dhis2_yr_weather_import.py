"""DHIS2 yr.no Weather Forecast Import.

Fetches point-based weather forecasts from the yr.no Locationforecast 2.0
API for each org unit centroid and imports daily values into DHIS2. Six
weather variables are produced:

1. **Temperature** (Celsius) -- daily mean ``air_temperature``
2. **Precipitation** (mm) -- daily sum ``precipitation_amount``
3. **Relative humidity** (%) -- daily mean ``relative_humidity``
4. **Wind speed** (m/s) -- daily mean ``wind_speed``
5. **Cloud cover** (%) -- daily mean ``cloud_area_fraction``
6. **Air pressure** (hPa) -- daily mean ``air_pressure_at_sea_level``

No API key required -- only a User-Agent header. The API provides up to
9 days of forecast data. Each run imports the current forecast window;
running regularly builds a historical record in DHIS2.
"""

from __future__ import annotations

import logging

from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect_climate import ClimateQuery, ImportResult, centroid
from prefect_climate.yr import DailyForecast, fetch_daily_forecasts
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

TEMPERATURE_DE_UID = "WfYrTmpEst1"  # Mean air temperature (Celsius)
PRECIPITATION_DE_UID = "WfYrPrcEst1"  # Total precipitation (mm)
HUMIDITY_DE_UID = "WfYrHumEst1"  # Relative humidity (%)
WIND_SPEED_DE_UID = "WfYrWndEst1"  # Wind speed (m/s)
CLOUD_COVER_DE_UID = "WfYrCldEst1"  # Cloud area fraction (%)
PRESSURE_DE_UID = "WfYrPrsEst1"  # Air pressure at sea level (hPa)

DATA_SET_UID = "WfYrFrcSet1"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(task_run_name="ensure-dhis2-metadata")
def ensure_dhis2_metadata(
    client: Dhis2Client,
    org_unit_level: int,
) -> list[OrgUnitGeo]:
    """Ensure data elements and data set exist; return org units with geometry.

    Creates six data elements (temperature, precipitation, humidity,
    wind speed, cloud cover, air pressure) and one daily weather forecast
    data set.

    Args:
        client: Authenticated DHIS2 client.
        org_unit_level: DHIS2 organisation unit hierarchy level.

    Returns:
        List of org units with polygon geometry.
    """
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
                name="WF: yr.no: Temperature",
                shortName="WF: yr.no: Temp",
                description="Daily mean air temperature from yr.no Locationforecast. Unit: degrees Celsius (C).",
            ),
            Dhis2DataElement(
                id=PRECIPITATION_DE_UID,
                name="WF: yr.no: Precipitation",
                shortName="WF: yr.no: Precip",
                description="Daily total precipitation from yr.no Locationforecast. Unit: millimetres (mm).",
            ),
            Dhis2DataElement(
                id=HUMIDITY_DE_UID,
                name="WF: yr.no: Relative Humidity",
                shortName="WF: yr.no: Rel Humidity",
                description="Daily mean relative humidity from yr.no Locationforecast. Unit: percent (%).",
            ),
            Dhis2DataElement(
                id=WIND_SPEED_DE_UID,
                name="WF: yr.no: Wind Speed",
                shortName="WF: yr.no: Wind Speed",
                description="Daily mean wind speed from yr.no Locationforecast. Unit: metres per second (m/s).",
            ),
            Dhis2DataElement(
                id=CLOUD_COVER_DE_UID,
                name="WF: yr.no: Cloud Cover",
                shortName="WF: yr.no: Cloud Cover",
                description="Daily mean cloud area fraction from yr.no Locationforecast. Unit: percent (%).",
            ),
            Dhis2DataElement(
                id=PRESSURE_DE_UID,
                name="WF: yr.no: Air Pressure",
                shortName="WF: yr.no: Air Pressure",
                description=(
                    "Daily mean air pressure at sea level from yr.no Locationforecast. Unit: hectopascals (hPa)."
                ),
            ),
        ],
        dataSets=[
            Dhis2DataSet(
                id=DATA_SET_UID,
                name="WF: yr.no: Weather Forecast",
                shortName="WF: yr.no: Forecast",
                description=(
                    "yr.no Locationforecast daily weather indicators:"
                    " temperature, precipitation, humidity, wind speed,"
                    " cloud cover, and air pressure."
                ),
                periodType="Daily",
                openFuturePeriods=10,
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
) -> tuple[OrgUnitGeo, list[DailyForecast]]:
    """Compute centroid of an org unit and fetch its weather forecast.

    Args:
        org_unit: Organisation unit with polygon geometry.

    Returns:
        Tuple of (org_unit, daily_forecasts).
    """
    lat, lon = centroid(org_unit.geometry)
    print(f"{org_unit.name}: fetching forecast for centroid ({lat:.4f}, {lon:.4f})")
    forecasts = fetch_daily_forecasts(lat, lon)
    print(f"{org_unit.name}: {len(forecasts)} forecast days")
    return (org_unit, forecasts)


@task(task_run_name="build-data-values")
def build_data_values(
    forecast_results: list[tuple[OrgUnitGeo, list[DailyForecast]]],
) -> Dhis2DataValueSet:
    """Build DHIS2 data values from forecast results.

    Creates one DataValue per org unit per day per variable.
    Period format: ``YYYYMMDD`` (DHIS2 daily).

    Args:
        forecast_results: List of (org_unit, forecasts) tuples.

    Returns:
        Dhis2DataValueSet containing all data values.
    """
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
            period = fc.date.replace("-", "")  # YYYY-MM-DD -> YYYYMMDD
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
    """POST data values to DHIS2 and return the import summary.

    Args:
        client: Authenticated DHIS2 client.
        dhis2_url: DHIS2 instance base URL.
        org_units: Org units included in the import.
        data_value_set: Data values to import.

    Returns:
        ImportResult with counts and markdown summary.
    """
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
        "## DHIS2 yr.no Weather Forecast Import",
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


@flow(name="dhis2_yr_weather_import", log_prints=True)
def dhis2_yr_weather_import_flow(
    query: ClimateQuery | None = None,
) -> ImportResult:
    """Fetch yr.no weather forecasts and import daily values into DHIS2.

    Computes the centroid of each org unit polygon, queries the yr.no
    Locationforecast API for that point, aggregates hourly data to daily
    values, and writes 6 weather indicators into a DHIS2 daily data set.

    The ``query.year`` and ``query.months`` parameters are not used --
    the forecast window is always the current time plus ~9 days.

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

    # -- Fetch forecast per org unit -----------------------------------------
    forecast_results: list[tuple[OrgUnitGeo, list[DailyForecast]]] = []
    for ou in org_units:
        ou_forecast = fetch_org_unit_forecast(ou)
        forecast_results.append(ou_forecast)

    data_value_set = build_data_values(forecast_results)
    import_result = import_to_dhis2(client, creds.base_url, org_units, data_value_set)

    create_markdown_artifact(key="dhis2-yr-weather-import", markdown=import_result.markdown)
    return import_result


if __name__ == "__main__":
    load_dotenv()
    dhis2_yr_weather_import_flow()
