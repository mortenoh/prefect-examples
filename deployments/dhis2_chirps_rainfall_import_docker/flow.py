"""DHIS2 CHIRPS Monthly Precipitation Import.

Downloads CHIRPS v3.0 daily GeoTIFFs, clips to a bounding box derived from
org unit geometries, aggregates daily to monthly totals, computes zonal mean
precipitation for each DHIS2 organisation unit, and writes monthly values
into DHIS2.
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect_climate import ClimateQuery, ClimateResult, ImportResult, bounding_box, zonal_mean
from prefect_climate.chirps import fetch_chirps_monthly
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

DATA_ELEMENT_UID = "PfChRnfEst1"
DATA_SET_UID = "PfChRnfSet1"


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
                name="PR: CHIRPS: Precipitation",
                shortName="PR: CHIRPS: Precipitation",
                description=(
                    "Monthly total precipitation from CHIRPS v3.0 (Climate Hazards"
                    " group InfraRed Precipitation with Station data)."
                    " Unit: millimetres (mm)."
                ),
            ),
        ],
        dataSets=[
            Dhis2DataSet(
                id=DATA_SET_UID,
                name="PR: CHIRPS: Precipitation",
                shortName="PR: CHIRPS: Precipitation",
                description="CHIRPS v3.0 monthly precipitation estimates at ~5 km resolution.",
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
    task_run_name="download-chirps-{year}-{month:02d}",
    retries=2,
    retry_delay_seconds=[5, 15],
)
def download_chirps_raster(
    year: int,
    month: int,
    area: list[float],
    cache_dir: Path,
) -> Path:
    """Download CHIRPS v3.0 daily data for one month, aggregate to monthly total.

    Args:
        year: Data year.
        month: Month number (1-12).
        area: Bounding box as ``[N, W, S, E]``.
        cache_dir: Local directory for cached downloads.

    Returns:
        Path to the monthly GeoTIFF file.
    """
    path = fetch_chirps_monthly(year, month, area, cache_dir)
    print(f"Downloaded CHIRPS {year}-{month:02d}: {path.name}")
    return path


@task(task_run_name="compute-precipitation-{org_unit.name}")
def compute_precipitation(
    org_unit: OrgUnitGeo,
    monthly_rasters: dict[int, Path],
) -> ClimateResult:
    """Compute zonal mean precipitation for each month.

    Args:
        org_unit: Organisation unit with polygon geometry.
        monthly_rasters: Dict mapping month number to GeoTIFF path.

    Returns:
        ClimateResult with monthly mean precipitation values.
    """
    monthly_values: dict[int, float] = {}
    for month, tiff_path in sorted(monthly_rasters.items()):
        mean_precip = zonal_mean(tiff_path, org_unit.geometry)
        monthly_values[month] = round(mean_precip, 1)
    total = sum(monthly_values.values())
    print(f"{org_unit.name}: {len(monthly_values)} months, total={total:.1f} mm")
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
    """Build DHIS2 data values from precipitation results.

    Creates one DataValue per org unit per month. Period format: ``YYYYMM``.

    Args:
        results: Precipitation results from zonal statistics.
        year: Data year for the period.

    Returns:
        Dhis2DataValueSet containing all data values.
    """
    values: list[DataValue] = []
    for r in results:
        for month, rain in r.monthly_values.items():
            values.append(
                DataValue(
                    dataElement=DATA_ELEMENT_UID,
                    period=f"{year}{month:02d}",
                    orgUnit=r.org_unit_id,
                    categoryOptionCombo="",
                    value=str(rain),
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
        "## DHIS2 CHIRPS Precipitation Import",
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
        "| Org Unit | Period | Value (mm) |",
        "|----------|--------|------------|",
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


@flow(name="dhis2_chirps_rainfall_import", log_prints=True)
def dhis2_chirps_rainfall_import_flow(
    query: ClimateQuery | None = None,
) -> ImportResult:
    """Fetch CHIRPS v3.0 monthly precipitation and import into DHIS2.

    Downloads CHIRPS v3.0 daily GeoTIFFs, clips to a bounding box derived
    from org unit geometries, aggregates to monthly totals, computes zonal
    mean precipitation for each org unit, and writes monthly values into
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

    cache_dir = Path(tempfile.gettempdir()) / "chirps_rainfall"

    monthly_rasters: dict[int, Path] = {}
    for month in query.months:
        path = download_chirps_raster(query.year, month, area, cache_dir)
        monthly_rasters[month] = path

    results: list[ClimateResult] = []
    for ou in org_units:
        climate_result = compute_precipitation(ou, monthly_rasters)
        results.append(climate_result)

    data_value_set = build_data_values(results, query.year)
    result = import_to_dhis2(client, creds.base_url, org_units, data_value_set)

    create_markdown_artifact(key="dhis2-chirps-rainfall-import", markdown=result.markdown)
    return result


if __name__ == "__main__":
    load_dotenv()
    dhis2_chirps_rainfall_import_flow.serve(
        name="dhis2-chirps-rainfall-import-docker",
    )
