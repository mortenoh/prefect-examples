"""DHIS2 WorldPop GeoTIFF Age-Sex Population Import.

Downloads WorldPop R2025A age/sex-disaggregated GeoTIFF rasters (40 per
country/year), extracts zonal population statistics for DHIS2 organisation unit
boundaries, and writes age-sex population values into DHIS2 using a
Sex x Age category combination (2 x 20 = 40 COCs).
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect_climate import ImportQuery, ImportResult
from prefect_climate.worldpop import (
    AGE_GROUPS,
    AgePopulationResult,
    build_age_tiff_url,
    download_tiff,
    zonal_population,
)
from prefect_dhis2 import (
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
    OrgUnitGeo,
    get_dhis2_credentials,
)
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants -- distinct UIDs so all flows can coexist
# ---------------------------------------------------------------------------

DATA_ELEMENT_UID = "PfAgPopEst1"
DATA_SET_UID = "PfAgPopSet1"
CAT_OPTION_MALE_UID = "PfAgSexMal1"
CAT_OPTION_FEMALE_UID = "PfAgSexFem1"
SEX_CATEGORY_UID = "PfAgSexCat1"
AGE_CATEGORY_UID = "PfAgAgeCat1"
CAT_COMBO_UID = "PfAgSxAgCC1"

# Age category option UIDs: PfAgAg00Op1, PfAgAg01Op1, PfAgAg05Op1, ..., PfAgAg90Op1
AGE_CAT_OPTION_UIDS: dict[int, str] = {age: f"PfAgAg{age:02d}Op1" for age in AGE_GROUPS}


# ---------------------------------------------------------------------------
# Local model (coc_mapping is dict, not CocMapping)
# ---------------------------------------------------------------------------


class AgeMetadataResult(BaseModel):
    """Metadata result with age-sex COC mapping."""

    org_units: list[OrgUnitGeo]
    coc_mapping: dict[str, str]  # keys like "M_0", "F_25" -> COC UID


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(task_run_name="ensure-dhis2-metadata")
def ensure_dhis2_metadata(
    client: Dhis2Client,
    org_unit_level: int,
) -> AgeMetadataResult:
    """Ensure age-sex category options, categories, combo, DE, and DS exist.

    Creates 22 category options (2 sex + 20 age), 2 categories, 1 category
    combo, 1 data element, and 1 data set. Resolves the 40 auto-generated
    categoryOptionCombos.

    Args:
        client: Authenticated DHIS2 client.
        org_unit_level: DHIS2 organisation unit hierarchy level.

    Returns:
        AgeMetadataResult with org units and COC mapping.
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

    # Build category options: 2 sex + 20 age
    sex_cat_options = [
        Dhis2CategoryOption(id=CAT_OPTION_MALE_UID, name="PR: AGE: Male", shortName="PR: AGE: Male"),
        Dhis2CategoryOption(id=CAT_OPTION_FEMALE_UID, name="PR: AGE: Female", shortName="PR: AGE: Female"),
    ]
    age_cat_options = [
        Dhis2CategoryOption(
            id=AGE_CAT_OPTION_UIDS[age],
            name=f"PR: AGE: {age}",
            shortName=f"PR: AGE: {age}",
        )
        for age in AGE_GROUPS
    ]

    payload = Dhis2MetadataPayload(
        categoryOptions=sex_cat_options + age_cat_options,
        categories=[
            Dhis2Category(
                id=SEX_CATEGORY_UID,
                name="PR: AGE: Sex",
                shortName="PR: AGE: Sex",
                categoryOptions=[
                    Dhis2Ref(id=CAT_OPTION_MALE_UID),
                    Dhis2Ref(id=CAT_OPTION_FEMALE_UID),
                ],
            ),
            Dhis2Category(
                id=AGE_CATEGORY_UID,
                name="PR: AGE: Age Group",
                shortName="PR: AGE: Age Group",
                categoryOptions=[Dhis2Ref(id=AGE_CAT_OPTION_UIDS[age]) for age in AGE_GROUPS],
            ),
        ],
        categoryCombos=[
            Dhis2CategoryCombo(
                id=CAT_COMBO_UID,
                name="PR: AGE: Sex x Age",
                categories=[Dhis2Ref(id=SEX_CATEGORY_UID), Dhis2Ref(id=AGE_CATEGORY_UID)],
            ),
        ],
        dataElements=[
            Dhis2DataElement(
                id=DATA_ELEMENT_UID,
                name="PR: AGE: Population",
                shortName="PR: AGE: Population",
                description=(
                    "Yearly population estimate from WorldPop age-sex GeoTIFF,"
                    " disaggregated by sex and 5-year age group."
                    " Unit: number of people."
                ),
                categoryCombo=Dhis2Ref(id=CAT_COMBO_UID),
            ),
        ],
        dataSets=[
            Dhis2DataSet(
                id=DATA_SET_UID,
                name="PR: AGE: Population",
                shortName="PR: AGE: Population",
                description=(
                    "WorldPop age-sex population estimates at ~100 m resolution,"
                    " disaggregated by sex and 5-year age group."
                ),
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

    # Resolve auto-generated categoryOptionCombos (40 total)
    cocs = client.fetch_metadata(
        "categoryOptionCombos",
        fields="id,name,categoryOptions[id]",
        filters=["categoryCombo.id:eq:" + CAT_COMBO_UID],
    )

    coc_mapping: dict[str, str] = {}
    for coc in cocs:
        option_ids = {co["id"] for co in coc.get("categoryOptions", [])}
        # Determine sex
        if CAT_OPTION_MALE_UID in option_ids:
            sex = "M"
        elif CAT_OPTION_FEMALE_UID in option_ids:
            sex = "F"
        else:
            continue
        # Determine age
        for age, uid in AGE_CAT_OPTION_UIDS.items():
            if uid in option_ids:
                coc_mapping[f"{sex}_{age}"] = coc["id"]
                break

    if len(coc_mapping) != 40:
        msg = f"Expected 40 COCs, resolved {len(coc_mapping)} for combo {CAT_COMBO_UID}"
        raise ValueError(msg)

    print(f"Resolved {len(coc_mapping)} COCs")
    return AgeMetadataResult(org_units=org_units, coc_mapping=coc_mapping)


@task(
    task_run_name="download-{sex}-{age:02d}-{iso3}-{year}",
    retries=2,
    retry_delay_seconds=[5, 15],
)
def download_single_raster(
    iso3: str,
    year: int,
    sex: str,
    age: int,
    cache_dir: Path,
) -> Path:
    """Download a single age/sex GeoTIFF raster.

    Args:
        iso3: ISO 3166-1 alpha-3 country code.
        year: Population year (2015-2030).
        sex: "M" or "F".
        age: Age group lower bound.
        cache_dir: Local directory for cached downloads.

    Returns:
        Path to the downloaded GeoTIFF file.
    """
    url = build_age_tiff_url(iso3, age, sex, year)
    path = download_tiff(url, cache_dir)
    print(f"Downloaded {path.name} ({path.stat().st_size / 1e6:.1f} MB)")
    return path


@task(task_run_name="compute-population-{org_unit.name}")
def compute_population(
    org_unit: OrgUnitGeo,
    rasters: dict[tuple[str, int], Path],
) -> AgePopulationResult:
    """Extract zonal population from all 40 rasters for a single org unit.

    Args:
        org_unit: Organisation unit with polygon geometry.
        rasters: Dict mapping (sex, age) to GeoTIFF paths.

    Returns:
        AgePopulationResult with population values for all 40 combos.
    """
    values: dict[str, float] = {}
    for (sex, age), tiff_path in rasters.items():
        pop = zonal_population(tiff_path, org_unit.geometry)
        values[f"{sex}_{age}"] = pop
    total = sum(values.values())
    print(f"{org_unit.name}: total={total:,.0f} across {len(values)} age-sex groups")
    return AgePopulationResult(
        org_unit_id=org_unit.id,
        org_unit_name=org_unit.name,
        values=values,
    )


@task(task_run_name="build-data-values-{year}")
def build_data_values(
    results: list[AgePopulationResult],
    year: int,
    coc_mapping: dict[str, str],
) -> Dhis2DataValueSet:
    """Build DHIS2 data values from age-sex population results.

    Creates 40 DataValues per org unit, each tagged with the appropriate
    categoryOptionCombo UID.

    Args:
        results: Population results from zonal statistics.
        year: Data year for the period.
        coc_mapping: Maps "M_0", "F_25" etc. to COC UIDs.

    Returns:
        Dhis2DataValueSet containing all data values.
    """
    values: list[DataValue] = []
    for r in results:
        for key, pop in r.values.items():
            coc_uid = coc_mapping.get(key)
            if coc_uid is None:
                continue
            values.append(
                DataValue(
                    dataElement=DATA_ELEMENT_UID,
                    period=str(year),
                    orgUnit=r.org_unit_id,
                    categoryOptionCombo=coc_uid,
                    value=str(round(pop)),
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
        "## DHIS2 WorldPop GeoTIFF Age-Sex Population Import",
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


@flow(name="dhis2_worldpop_geotiff_age_import", log_prints=True)
def dhis2_worldpop_geotiff_age_import_flow(
    query: ImportQuery | None = None,
) -> ImportResult:
    """Fetch WorldPop age-sex GeoTIFF population data and import into DHIS2.

    Downloads R2025A age/sex-disaggregated rasters (40 per country/year),
    extracts zonal population for each org unit, and writes results into
    DHIS2 using a Sex x Age category combination.

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

    cache_dir = Path(tempfile.gettempdir()) / "worldpop_geotiff_age"

    all_data_value_sets: list[Dhis2DataValueSet] = []
    for year in query.years:
        rasters: dict[tuple[str, int], Path] = {}
        for sex in ("M", "F"):
            for age in AGE_GROUPS:
                path = download_single_raster(query.iso3, year, sex, age, cache_dir)
                rasters[(sex, age)] = path

        results: list[AgePopulationResult] = []
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

    create_markdown_artifact(key="dhis2-worldpop-geotiff-age-import", markdown=result.markdown)
    return result


if __name__ == "__main__":
    load_dotenv()
    dhis2_worldpop_geotiff_age_import_flow.serve(
        name="dhis2-worldpop-geotiff-age-import-docker",
    )
