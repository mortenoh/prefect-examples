"""DHIS2 WorldPop GeoTIFF Population Import.

Downloads WorldPop R2025A sex-disaggregated GeoTIFF rasters, extracts zonal
population statistics for DHIS2 organisation unit boundaries, and writes
male/female population values into DHIS2 using category combinations.

Unlike the API-based flow (dhis2_worldpop_population_import), this flow uses
pre-aggregated total-male / total-female summary rasters that cover 231
countries (2015-2030) with full sex disaggregation -- no fallback needed.
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect_dhis2 import (
    CocMapping,
    DataValue,
    Dhis2Category,
    Dhis2CategoryCombo,
    Dhis2CategoryOption,
    Dhis2Client,
    Dhis2DataElement,
    Dhis2DataSet,
    Dhis2DataSetElement,
    Dhis2DataValueSet,
    Dhis2MetadataPayload,
    Dhis2Ref,
    MetadataResult,
    OrgUnitGeo,
    get_dhis2_credentials,
)
from prefect_worldpop import ImportQuery, ImportResult, RasterPair, WorldPopResult
from prefect_worldpop.geotiff import download_sex_rasters, population_by_sex

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants -- distinct UIDs so both flows can coexist
# ---------------------------------------------------------------------------

DATA_ELEMENT_UID = "PfGtPopEst1"
DATA_SET_UID = "PfGtPopSet1"
CAT_OPTION_MALE_UID = "PfGtSexMal1"
CAT_OPTION_FEMALE_UID = "PfGtSexFem1"
CATEGORY_UID = "PfGtSexCat1"
CAT_COMBO_UID = "PfGtSexCCo1"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(task_run_name="ensure-dhis2-metadata")
def ensure_dhis2_metadata(
    client: Dhis2Client,
    org_unit_level: int,
) -> MetadataResult:
    """Ensure category options, category, category combo, DE, and DS exist.

    Fetches org units at the specified level with polygon geometry for
    zonal statistics extraction.

    Args:
        client: Authenticated DHIS2 client.
        org_unit_level: DHIS2 organisation unit hierarchy level.

    Returns:
        MetadataResult with org units and COC mapping.
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
        categoryOptions=[
            Dhis2CategoryOption(id=CAT_OPTION_MALE_UID, name="PR: GT: Male", shortName="PR: GT: Male"),
            Dhis2CategoryOption(id=CAT_OPTION_FEMALE_UID, name="PR: GT: Female", shortName="PR: GT: Female"),
        ],
        categories=[
            Dhis2Category(
                id=CATEGORY_UID,
                name="PR: GT: Sex",
                shortName="PR: GT: Sex",
                categoryOptions=[Dhis2Ref(id=CAT_OPTION_MALE_UID), Dhis2Ref(id=CAT_OPTION_FEMALE_UID)],
            ),
        ],
        categoryCombos=[
            Dhis2CategoryCombo(
                id=CAT_COMBO_UID,
                name="PR: GT: Sex",
                categories=[Dhis2Ref(id=CATEGORY_UID)],
            ),
        ],
        dataElements=[
            Dhis2DataElement(
                id=DATA_ELEMENT_UID,
                name="PR: GT: WorldPop GeoTIFF Population",
                shortName="PR: GT: WP GeoTIFF Pop",
                categoryCombo=Dhis2Ref(id=CAT_COMBO_UID),
            ),
        ],
        dataSets=[
            Dhis2DataSet(
                id=DATA_SET_UID,
                name="PR: GT: WorldPop GeoTIFF Population",
                shortName="PR: GT: WP GeoTIFF Pop",
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
        if CAT_OPTION_MALE_UID in option_ids:
            male_coc_uid = coc["id"]
        elif CAT_OPTION_FEMALE_UID in option_ids:
            female_coc_uid = coc["id"]

    if not male_coc_uid or not female_coc_uid:
        msg = f"Could not resolve COCs for category combo {CAT_COMBO_UID}"
        raise ValueError(msg)

    print(f"Resolved COCs: male={male_coc_uid}, female={female_coc_uid}")
    return MetadataResult(
        org_units=org_units,
        coc_mapping=CocMapping(male=male_coc_uid, female=female_coc_uid),
    )


@task(
    task_run_name="download-worldpop-rasters-{iso3}-{year}",
    retries=2,
    retry_delay_seconds=[5, 15],
)
def download_worldpop_rasters(
    iso3: str,
    year: int,
    cache_dir: Path,
) -> RasterPair:
    """Download male and female GeoTIFF rasters for a country/year.

    Args:
        iso3: ISO 3166-1 alpha-3 country code.
        year: Population year (2015-2030).
        cache_dir: Local directory for cached downloads.

    Returns:
        RasterPair with male and female file paths.
    """
    print(f"Downloading WorldPop rasters for {iso3}/{year}...")
    male_path, female_path = download_sex_rasters(iso3, year, cache_dir)
    print(f"Downloaded: {male_path.name}, {female_path.name}")
    return RasterPair(male=male_path, female=female_path)


@task(task_run_name="compute-population-{org_unit.name}")
def compute_population(
    org_unit: OrgUnitGeo,
    rasters: RasterPair,
) -> WorldPopResult:
    """Extract zonal population from GeoTIFF rasters for a single org unit.

    Clips each raster to the org unit polygon boundary and sums pixel values
    to get the total male and female population.

    Args:
        org_unit: Organisation unit with polygon geometry.
        rasters: Paths to male and female GeoTIFF rasters.

    Returns:
        WorldPopResult with male and female population totals.
    """
    male, female = population_by_sex(rasters.male, rasters.female, org_unit.geometry)
    print(f"{org_unit.name}: male={male:,.0f}, female={female:,.0f}")
    return WorldPopResult(
        org_unit_id=org_unit.id,
        org_unit_name=org_unit.name,
        male=male,
        female=female,
    )


@task(task_run_name="build-data-values-{year}")
def build_data_values(
    results: list[WorldPopResult],
    year: int,
    coc_mapping: CocMapping,
) -> Dhis2DataValueSet:
    """Build DHIS2 data values from population results.

    Creates two DataValues per org unit (male + female), each tagged with the
    appropriate categoryOptionCombo UID.

    Args:
        results: Population results from zonal statistics.
        year: Data year for the period.
        coc_mapping: Resolved COC UIDs for male and female.

    Returns:
        Dhis2DataValueSet containing all data values.
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

    # DHIS2 may nest counts under "response" or at the top level
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
        "## DHIS2 WorldPop GeoTIFF Population Import",
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


@flow(name="dhis2_worldpop_geotiff_import", log_prints=True)
def dhis2_worldpop_geotiff_import_flow(
    query: ImportQuery | None = None,
) -> ImportResult:
    """Fetch WorldPop GeoTIFF population data by sex and import into DHIS2.

    Downloads R2025A total-male/total-female summary rasters, extracts zonal
    population for each org unit at the specified hierarchy level, and writes
    results into DHIS2 using category combinations.

    Args:
        query: Query parameters (iso3, org_unit_level, years).

    Returns:
        ImportResult with counts and markdown summary.
    """
    if query is None:
        query = ImportQuery(iso3="SLE")
        print(f"No query provided, using default: iso3={query.iso3}, level={query.org_unit_level}, years={query.years}")

    creds = get_dhis2_credentials()
    print(f"DHIS2 target: {creds.base_url}")
    client = creds.get_client()

    metadata = ensure_dhis2_metadata(client, query.org_unit_level)

    cache_dir = Path(tempfile.gettempdir()) / "worldpop_geotiff"

    all_data_value_sets: list[Dhis2DataValueSet] = []
    for year in query.years:
        rasters = download_worldpop_rasters(query.iso3, year, cache_dir)

        results: list[WorldPopResult] = []
        for ou in metadata.org_units:
            wp_result = compute_population(ou, rasters)
            results.append(wp_result)

        data_value_set = build_data_values(results, year, metadata.coc_mapping)
        all_data_value_sets.append(data_value_set)

    # Merge all year data value sets into one
    all_values: list[DataValue] = []
    for dvs in all_data_value_sets:
        all_values.extend(dvs.dataValues)
    combined = Dhis2DataValueSet(dataValues=all_values)

    result = import_to_dhis2(client, creds.base_url, metadata.org_units, combined)

    create_markdown_artifact(key="dhis2-worldpop-geotiff-import", markdown=result.markdown)
    return result


if __name__ == "__main__":
    load_dotenv()
    dhis2_worldpop_geotiff_import_flow()
