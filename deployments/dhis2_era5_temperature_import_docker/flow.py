"""DHIS2 ERA5-Land Monthly Temperature Import.

Downloads ERA5-Land monthly-mean 2m temperature via earthkit-data, computes
zonal mean temperature for each DHIS2 organisation unit, and writes monthly
values into DHIS2.

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
from prefect_climate import ClimateQuery, ClimateResult, ImportResult, bounding_box, zonal_mean
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

DATA_ELEMENT_UID = "PfE5TmpEst1"
DATA_SET_UID = "PfE5TmpSet1"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(task_run_name="ensure-dhis2-metadata")
def ensure_dhis2_metadata(
    client: Dhis2Client,
    org_unit_level: int,
) -> list[OrgUnitGeo]:
    """Ensure data element and data set exist; return org units with geometry.

    No category combos needed -- monthly periods handle the time dimension.

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
                id=DATA_ELEMENT_UID,
                name="PR: ERA5: Mean Temperature",
                shortName="PR: ERA5: Mean Temp",
            ),
        ],
        dataSets=[
            Dhis2DataSet(
                id=DATA_SET_UID,
                name="PR: ERA5: Temperature",
                shortName="PR: ERA5: Temperature",
                periodType="Monthly",
                dataSetElements=[Dhis2DataSetElement(dataElement=Dhis2Ref(id=DATA_ELEMENT_UID))],
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
    task_run_name="download-era5-temperature-{year}",
    retries=2,
    retry_delay_seconds=[5, 15],
)
def download_era5_temperature(
    year: int,
    months: list[int],
    area: list[float],
    cache_dir: Path,
) -> dict[int, Path]:
    """Download ERA5-Land monthly-mean 2m temperature as GeoTIFF per month.

    Args:
        year: Data year.
        months: List of month numbers (1-12).
        area: Bounding box [N, W, S, E].
        cache_dir: Local directory for cached downloads.

    Returns:
        Dict mapping month number to local GeoTIFF path.
    """
    print(f"Downloading ERA5-Land 2m_temperature for {year}, months={months}")
    rasters = fetch_era5_monthly("2m_temperature", year, months, area, cache_dir)
    print(f"Downloaded {len(rasters)} monthly rasters")
    return rasters


@task(task_run_name="compute-temperature-{org_unit.name}")
def compute_temperature(
    org_unit: OrgUnitGeo,
    monthly_rasters: dict[int, Path],
) -> ClimateResult:
    """Compute zonal mean temperature for each month.

    Args:
        org_unit: Organisation unit with polygon geometry.
        monthly_rasters: Dict mapping month number to GeoTIFF path.

    Returns:
        ClimateResult with monthly mean temperature values.
    """
    monthly_values: dict[int, float] = {}
    for month, tiff_path in sorted(monthly_rasters.items()):
        mean_temp = zonal_mean(tiff_path, org_unit.geometry)
        monthly_values[month] = round(mean_temp, 1)
    print(
        f"{org_unit.name}: {len(monthly_values)} months, avg={sum(monthly_values.values()) / len(monthly_values):.1f} C"
    )
    return ClimateResult(
        org_unit_id=org_unit.id,
        org_unit_name=org_unit.name,
        monthly_values=monthly_values,
    )


@task(task_run_name="build-data-values-{year}")
def build_data_values(
    results: list[ClimateResult],
    year: int,
) -> Dhis2DataValueSet:
    """Build DHIS2 data values from temperature results.

    Creates one DataValue per org unit per month. Period format: ``YYYYMM``.

    Args:
        results: Temperature results from zonal statistics.
        year: Data year for the period.

    Returns:
        Dhis2DataValueSet containing all data values.
    """
    values: list[DataValue] = []
    for r in results:
        for month, temp in r.monthly_values.items():
            values.append(
                DataValue(
                    dataElement=DATA_ELEMENT_UID,
                    period=f"{year}{month:02d}",
                    orgUnit=r.org_unit_id,
                    categoryOptionCombo="",
                    value=str(temp),
                )
            )
    print(f"Built {len(values)} data values for {len(results)} org units")
    return Dhis2DataValueSet(dataValues=values)


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
        "## DHIS2 ERA5-Land Temperature Import",
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
        "| Org Unit | Period | Value (C) |",
        "|----------|--------|-----------|",
    ]
    for dv in sorted(data_values, key=lambda d: (d.orgUnit, d.period)):
        lines.append(f"| {dv.orgUnit} | {dv.period} | {dv.value} |")
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


@flow(name="dhis2_era5_temperature_import", log_prints=True)
def dhis2_era5_temperature_import_flow(
    query: ClimateQuery | None = None,
) -> ImportResult:
    """Fetch ERA5-Land monthly temperature and import into DHIS2.

    Downloads ERA5-Land 2m temperature via earthkit-data, computes zonal
    mean for each org unit, and writes monthly values into DHIS2.

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

    cache_dir = Path(tempfile.gettempdir()) / "era5_temperature"
    monthly_rasters = download_era5_temperature(query.year, query.months, area, cache_dir)

    results: list[ClimateResult] = []
    for ou in org_units:
        climate_result = compute_temperature(ou, monthly_rasters)
        results.append(climate_result)

    data_value_set = build_data_values(results, query.year)
    result = import_to_dhis2(client, creds.base_url, org_units, data_value_set)

    create_markdown_artifact(key="dhis2-era5-temperature-import", markdown=result.markdown)
    return result


if __name__ == "__main__":
    load_dotenv()
    dhis2_era5_temperature_import_flow.serve(
        name="dhis2-era5-temperature-import-docker",
    )
