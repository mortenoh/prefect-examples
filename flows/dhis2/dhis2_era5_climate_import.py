"""DHIS2 ERA5-Land Monthly Climate Import.

Downloads ERA5-Land monthly-mean 2m temperature, total precipitation, and 2m
dewpoint temperature via earthkit-data. Computes zonal mean temperature and
precipitation, derives relative humidity from temperature and dewpoint, and
writes monthly values into DHIS2.

Requires CDS API credentials set via environment variables:
- ``CDSAPI_URL`` (default: ``https://cds.climate.copernicus.eu/api``)
- ``CDSAPI_KEY`` (user's CDS API key)
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect_climate import (
    ClimateQuery,
    ImportResult,
    bounding_box,
    relative_humidity,
    zonal_mean,
)
from prefect_climate.era5 import fetch_era5_monthly
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

TEMPERATURE_DE_UID = "PfE5TmpEst1"
PRECIPITATION_DE_UID = "PfE5PrcEst1"
HUMIDITY_DE_UID = "PfE5HumEst1"
DATA_SET_UID = "PfE5ClmSet1"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(task_run_name="ensure-dhis2-metadata")
def ensure_dhis2_metadata(
    client: Dhis2Client,
    org_unit_level: int,
) -> list[OrgUnitGeo]:
    """Ensure data elements and data set exist; return org units with geometry.

    Creates three data elements (temperature, precipitation, humidity) and one
    unified climate data set. No category combos needed -- monthly periods
    handle the time dimension.

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
                name="PR: ERA5: Mean Temperature",
                shortName="PR: ERA5: Mean Temp",
            ),
            Dhis2DataElement(
                id=PRECIPITATION_DE_UID,
                name="PR: ERA5: Total Precipitation",
                shortName="PR: ERA5: Total Precip",
            ),
            Dhis2DataElement(
                id=HUMIDITY_DE_UID,
                name="PR: ERA5: Relative Humidity",
                shortName="PR: ERA5: Rel Humidity",
            ),
        ],
        dataSets=[
            Dhis2DataSet(
                id=DATA_SET_UID,
                name="PR: ERA5: Climate",
                shortName="PR: ERA5: Climate",
                periodType="Monthly",
                dataSetElements=[
                    Dhis2DataSetElement(dataElement=Dhis2Ref(id=TEMPERATURE_DE_UID)),
                    Dhis2DataSetElement(dataElement=Dhis2Ref(id=PRECIPITATION_DE_UID)),
                    Dhis2DataSetElement(dataElement=Dhis2Ref(id=HUMIDITY_DE_UID)),
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
    task_run_name="download-era5-{variable}-{year}",
    retries=2,
    retry_delay_seconds=[5, 15],
)
def download_era5_variable(
    variable: str,
    year: int,
    months: list[int],
    area: list[float],
    cache_dir: Path,
) -> dict[int, Path]:
    """Download ERA5-Land monthly-mean variable as GeoTIFF per month.

    Args:
        variable: CDS variable name (e.g. ``"2m_temperature"``).
        year: Data year.
        months: List of month numbers (1-12).
        area: Bounding box [N, W, S, E].
        cache_dir: Local directory for cached downloads.

    Returns:
        Dict mapping month number to local GeoTIFF path.
    """
    print(f"Downloading ERA5-Land {variable} for {year}, months={months}")
    rasters = fetch_era5_monthly(variable, year, months, area, cache_dir)
    print(f"Downloaded {len(rasters)} monthly rasters for {variable}")
    return rasters


@task(task_run_name="compute-climate-{org_unit.name}")
def compute_climate(
    org_unit: OrgUnitGeo,
    temp_rasters: dict[int, Path],
    precip_rasters: dict[int, Path],
    dewpoint_rasters: dict[int, Path],
) -> dict[str, dict[int, float]]:
    """Compute zonal climate stats for each month.

    Computes mean temperature, total precipitation, and derives relative
    humidity from temperature and dewpoint using the Magnus formula.

    Args:
        org_unit: Organisation unit with polygon geometry.
        temp_rasters: Temperature GeoTIFFs per month.
        precip_rasters: Precipitation GeoTIFFs per month.
        dewpoint_rasters: Dewpoint GeoTIFFs per month.

    Returns:
        Dict with keys ``"temperature"``, ``"precipitation"``,
        ``"humidity"``, each mapping month number to value.
    """
    temperature: dict[int, float] = {}
    precipitation: dict[int, float] = {}
    humidity: dict[int, float] = {}

    months = sorted(temp_rasters.keys())
    for month in months:
        temp_val = zonal_mean(temp_rasters[month], org_unit.geometry)
        temperature[month] = round(temp_val, 1)

        precip_val = zonal_mean(precip_rasters[month], org_unit.geometry)
        precipitation[month] = round(precip_val, 1)

        dewpoint_val = zonal_mean(dewpoint_rasters[month], org_unit.geometry)
        rh = relative_humidity(temp_val, dewpoint_val)
        humidity[month] = round(rh, 1)

    print(
        f"{org_unit.name}: {len(months)} months, "
        f"avg temp={sum(temperature.values()) / len(temperature):.1f} C, "
        f"avg precip={sum(precipitation.values()) / len(precipitation):.1f} mm, "
        f"avg RH={sum(humidity.values()) / len(humidity):.1f} %"
    )
    return {
        "temperature": temperature,
        "precipitation": precipitation,
        "humidity": humidity,
    }


@task(task_run_name="build-data-values-{year}")
def build_data_values(
    climate_results: list[tuple[OrgUnitGeo, dict[str, dict[int, float]]]],
    year: int,
) -> Dhis2DataValueSet:
    """Build DHIS2 data values from climate results.

    Creates one DataValue per org unit per month per variable.
    Period format: ``YYYYMM``.

    Args:
        climate_results: List of (org_unit, climate_dict) tuples.
        year: Data year for the period.

    Returns:
        Dhis2DataValueSet containing all data values.
    """
    de_map = {
        "temperature": TEMPERATURE_DE_UID,
        "precipitation": PRECIPITATION_DE_UID,
        "humidity": HUMIDITY_DE_UID,
    }

    values: list[DataValue] = []
    for ou, climate_data in climate_results:
        for var_key, de_uid in de_map.items():
            for month, val in climate_data[var_key].items():
                values.append(
                    DataValue(
                        dataElement=de_uid,
                        period=f"{year}{month:02d}",
                        orgUnit=ou.id,
                        categoryOptionCombo="",
                        value=str(val),
                    )
                )
    print(f"Built {len(values)} data values for {len(climate_results)} org units")
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
        "## DHIS2 ERA5-Land Climate Import",
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


@flow(name="dhis2_era5_climate_import", log_prints=True)
def dhis2_era5_climate_import_flow(
    query: ClimateQuery | None = None,
) -> ImportResult:
    """Fetch ERA5-Land monthly climate data and import into DHIS2.

    Downloads ERA5-Land 2m temperature, total precipitation, and 2m dewpoint
    temperature via earthkit-data. Computes zonal mean temperature and
    precipitation, derives relative humidity, and writes monthly values into
    DHIS2.

    Args:
        query: Query parameters (iso3, org_unit_level, year, months).

    Returns:
        ImportResult with counts and markdown summary.
    """
    if query is None:
        query = ClimateQuery()
        print(f"No query provided, using default: iso3={query.iso3}, year={query.year}, months={query.months}")

    creds = get_dhis2_credentials()
    print(f"DHIS2 target: {creds.base_url}")
    client = creds.get_client()

    org_units = ensure_dhis2_metadata(client, query.org_unit_level)

    area = bounding_box(org_units)
    print(f"Bounding box: N={area[0]}, W={area[1]}, S={area[2]}, E={area[3]}")

    cache_dir = Path(tempfile.gettempdir()) / "era5_climate"

    temp_rasters = download_era5_variable(
        "2m_temperature",
        query.year,
        query.months,
        area,
        cache_dir,
    )
    precip_rasters = download_era5_variable(
        "total_precipitation",
        query.year,
        query.months,
        area,
        cache_dir,
    )
    dewpoint_rasters = download_era5_variable(
        "2m_dewpoint_temperature",
        query.year,
        query.months,
        area,
        cache_dir,
    )

    climate_results: list[tuple[OrgUnitGeo, dict[str, dict[int, float]]]] = []
    for ou in org_units:
        climate_data = compute_climate(ou, temp_rasters, precip_rasters, dewpoint_rasters)
        climate_results.append((ou, climate_data))

    data_value_set = build_data_values(climate_results, query.year)
    result = import_to_dhis2(client, creds.base_url, org_units, data_value_set)

    create_markdown_artifact(key="dhis2-era5-climate-import", markdown=result.markdown)
    return result


if __name__ == "__main__":
    load_dotenv()
    dhis2_era5_climate_import_flow()
