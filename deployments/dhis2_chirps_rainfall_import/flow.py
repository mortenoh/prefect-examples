"""DHIS2 CHIRPS Monthly Rainfall Import.

Downloads CHIRPS v2.0 Africa monthly rainfall GeoTIFFs, computes zonal mean
rainfall for each DHIS2 organisation unit, and writes monthly values into DHIS2.
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect_climate import ClimateQuery, ClimateResult, ImportResult, zonal_mean
from prefect_climate.chirps import build_chirps_url, download_chirps
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
                name="PR: CHIRPS: Rainfall",
                shortName="PR: CHIRPS: Rainfall",
            ),
        ],
        dataSets=[
            Dhis2DataSet(
                id=DATA_SET_UID,
                name="PR: CHIRPS: Rainfall",
                shortName="PR: CHIRPS: Rainfall",
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
    cache_dir: Path,
) -> Path:
    """Download a single CHIRPS monthly rainfall GeoTIFF.

    Args:
        year: Data year.
        month: Month number (1-12).
        cache_dir: Local directory for cached downloads.

    Returns:
        Path to the decompressed GeoTIFF file.
    """
    url = build_chirps_url(year, month)
    path = download_chirps(url, cache_dir)
    print(f"Downloaded CHIRPS {year}-{month:02d}: {path.name}")
    return path


@task(task_run_name="compute-rainfall-{org_unit.name}")
def compute_rainfall(
    org_unit: OrgUnitGeo,
    monthly_rasters: dict[int, Path],
) -> ClimateResult:
    """Compute zonal mean rainfall for each month.

    Args:
        org_unit: Organisation unit with polygon geometry.
        monthly_rasters: Dict mapping month number to GeoTIFF path.

    Returns:
        ClimateResult with monthly mean rainfall values.
    """
    monthly_values: dict[int, float] = {}
    for month, tiff_path in sorted(monthly_rasters.items()):
        mean_rain = zonal_mean(tiff_path, org_unit.geometry)
        monthly_values[month] = round(mean_rain, 1)
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
    """Build DHIS2 data values from rainfall results.

    Creates one DataValue per org unit per month. Period format: ``YYYYMM``.

    Args:
        results: Rainfall results from zonal statistics.
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
        "## DHIS2 CHIRPS Rainfall Import",
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
    """Fetch CHIRPS monthly rainfall and import into DHIS2.

    Downloads CHIRPS v2.0 Africa monthly GeoTIFFs, computes zonal mean
    rainfall for each org unit, and writes monthly values into DHIS2.

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

    cache_dir = Path(tempfile.gettempdir()) / "chirps_rainfall"

    monthly_rasters: dict[int, Path] = {}
    for month in query.months:
        path = download_chirps_raster(query.year, month, cache_dir)
        monthly_rasters[month] = path

    results: list[ClimateResult] = []
    for ou in org_units:
        climate_result = compute_rainfall(ou, monthly_rasters)
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
