"""Tests for DHIS2 WorldPop GeoTIFF Age-Sex Population Import flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Load the flow module via importlib (same pattern as existing tests)
# ---------------------------------------------------------------------------

_spec = importlib.util.spec_from_file_location(
    "dhis2_worldpop_geotiff_age_import",
    Path(__file__).resolve().parent.parent.parent / "flows" / "dhis2" / "dhis2_worldpop_geotiff_age_import.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["dhis2_worldpop_geotiff_age_import"] = _mod
_spec.loader.exec_module(_mod)

from prefect_dhis2 import (  # noqa: E402
    DataValue,
    Dhis2Client,
    Dhis2DataValueSet,
    OrgUnitGeo,
)
from prefect_dhis2.credentials import Dhis2Credentials  # noqa: E402
from prefect_worldpop import AgePopulationResult, ImportQuery, ImportResult  # noqa: E402
from prefect_worldpop.geotiff import AGE_GROUPS, build_age_tiff_url  # noqa: E402

ensure_dhis2_metadata = _mod.ensure_dhis2_metadata
download_single_raster = _mod.download_single_raster
compute_population = _mod.compute_population
build_data_values = _mod.build_data_values
import_to_dhis2 = _mod.import_to_dhis2
dhis2_worldpop_geotiff_age_import_flow = _mod.dhis2_worldpop_geotiff_age_import_flow

AgeMetadataResult = _mod.AgeMetadataResult
AGE_CAT_OPTION_UIDS = _mod.AGE_CAT_OPTION_UIDS
CAT_OPTION_MALE_UID = _mod.CAT_OPTION_MALE_UID
CAT_OPTION_FEMALE_UID = _mod.CAT_OPTION_FEMALE_UID

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

SAMPLE_OU_WITH_GEOM = [
    {
        "id": "ROOT_OU",
        "name": "Freetown",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-13.3, 8.4], [-13.2, 8.4], [-13.2, 8.5], [-13.3, 8.5], [-13.3, 8.4]]],
        },
    }
]

SAMPLE_OU_POINT = [
    {
        "id": "POINT_OU",
        "name": "Point Org",
        "geometry": {"type": "Point", "coordinates": [-13.3, 8.4]},
    }
]


def _build_sample_cocs() -> list[dict[str, object]]:
    """Build 40 sample COCs for sex x age."""
    cocs: list[dict[str, object]] = []
    for sex_label, sex_uid in [("M", CAT_OPTION_MALE_UID), ("F", CAT_OPTION_FEMALE_UID)]:
        for age in AGE_GROUPS:
            cocs.append(
                {
                    "id": f"COC_{sex_label}_{age}",
                    "name": f"{sex_label} {age}",
                    "categoryOptions": [
                        {"id": sex_uid},
                        {"id": AGE_CAT_OPTION_UIDS[age]},
                    ],
                }
            )
    return cocs


SAMPLE_COCS = _build_sample_cocs()

SAMPLE_METADATA_RESPONSE = {
    "status": "OK",
    "stats": {"created": 26, "updated": 0, "deleted": 0, "ignored": 0, "total": 26},
}

SAMPLE_IMPORT_RESPONSE = {
    "status": "SUCCESS",
    "importCount": {"imported": 40, "updated": 0, "ignored": 0, "deleted": 0},
}


# ---------------------------------------------------------------------------
# Library tests
# ---------------------------------------------------------------------------


def test_build_age_tiff_url() -> None:
    url = build_age_tiff_url("SLE", 0, "M", 2024)
    assert url == (
        "https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/"
        "R2025A/2024/SLE/v1/100m/constrained/sle_m_00_2024_CN_100m_R2025A_v1.tif"
    )


def test_build_age_tiff_url_female() -> None:
    url = build_age_tiff_url("SLE", 25, "F", 2020)
    assert url == (
        "https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/"
        "R2025A/2020/SLE/v1/100m/constrained/sle_f_25_2020_CN_100m_R2025A_v1.tif"
    )


def test_build_age_tiff_url_case_handling() -> None:
    url = build_age_tiff_url("sle", 10, "M", 2024)
    assert "/SLE/" in url
    assert "sle_m_10_" in url


def test_age_groups_count() -> None:
    assert len(AGE_GROUPS) == 20
    assert AGE_GROUPS[0] == 0
    assert AGE_GROUPS[1] == 1
    assert AGE_GROUPS[-1] == 90


# ---------------------------------------------------------------------------
# Task tests
# ---------------------------------------------------------------------------


def test_ensure_dhis2_metadata() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.side_effect = [
        SAMPLE_OU_WITH_GEOM,
        SAMPLE_COCS,
    ]
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE

    metadata = ensure_dhis2_metadata.fn(mock_client, 2)

    assert isinstance(metadata, AgeMetadataResult)
    assert len(metadata.org_units) == 1
    assert metadata.org_units[0].id == "ROOT_OU"
    assert metadata.org_units[0].name == "Freetown"
    assert len(metadata.coc_mapping) == 40

    # Verify all 40 COC keys present
    for sex in ("M", "F"):
        for age in AGE_GROUPS:
            key = f"{sex}_{age}"
            assert key in metadata.coc_mapping, f"Missing COC key: {key}"
            assert metadata.coc_mapping[key] == f"COC_{sex}_{age}"

    # Verify metadata payload structure
    mock_client.post_metadata.assert_called_once()
    payload = mock_client.post_metadata.call_args[0][0]
    assert "categoryOptions" in payload
    assert "categories" in payload
    assert "categoryCombos" in payload
    assert "dataElements" in payload
    assert "dataSets" in payload
    assert len(payload["categoryOptions"]) == 22  # 2 sex + 20 age
    assert len(payload["categories"]) == 2  # sex + age
    assert len(payload["categoryCombos"]) == 1
    assert len(payload["dataElements"]) == 1
    assert payload["dataElements"][0]["name"] == "PR: AGE: Population"
    assert len(payload["dataSets"]) == 1
    assert payload["dataSets"][0]["organisationUnits"] == [{"id": "ROOT_OU"}]
    mock_client.run_maintenance.assert_called_once_with("categoryOptionComboUpdate")


def test_ensure_dhis2_metadata_no_polygon() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_OU_POINT

    with pytest.raises(ValueError, match="No level-2 org units with Polygon"):
        ensure_dhis2_metadata.fn(mock_client, 2)


def test_ensure_dhis2_metadata_missing_cocs() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.side_effect = [
        SAMPLE_OU_WITH_GEOM,
        [],  # no COCs returned
    ]
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE

    with pytest.raises(ValueError, match="Expected 40 COCs"):
        ensure_dhis2_metadata.fn(mock_client, 2)


@patch("dhis2_worldpop_geotiff_age_import.zonal_population")
def test_compute_population(mock_zonal: MagicMock) -> None:
    mock_zonal.return_value = 500.0

    ou = OrgUnitGeo(
        id="ROOT_OU",
        name="Freetown",
        geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"],
    )
    # Build 40 rasters
    rasters = {(sex, age): Path(f"{sex}_{age}.tif") for sex in ("M", "F") for age in AGE_GROUPS}
    result = compute_population.fn(ou, rasters)

    assert isinstance(result, AgePopulationResult)
    assert result.org_unit_id == "ROOT_OU"
    assert result.org_unit_name == "Freetown"
    assert len(result.values) == 40
    assert all(v == 500.0 for v in result.values.values())
    assert mock_zonal.call_count == 40


def test_build_data_values() -> None:
    values = {f"{sex}_{age}": 100.0 for sex in ("M", "F") for age in AGE_GROUPS}
    results = [
        AgePopulationResult(org_unit_id="ROOT_OU", org_unit_name="Freetown", values=values),
    ]
    coc_mapping = {f"{sex}_{age}": f"COC_{sex}_{age}" for sex in ("M", "F") for age in AGE_GROUPS}

    dvs = build_data_values.fn(results, 2024, coc_mapping)

    assert isinstance(dvs, Dhis2DataValueSet)
    assert len(dvs.dataValues) == 40
    # Verify each DV has correct fields
    for dv in dvs.dataValues:
        assert dv.dataElement == "PfAgPopEst1"
        assert dv.period == "2024"
        assert dv.orgUnit == "ROOT_OU"
        assert dv.value == "100"
        assert dv.categoryOptionCombo.startswith("COC_")


def test_build_data_values_multiple_org_units() -> None:
    values = {f"{sex}_{age}": 50.0 for sex in ("M", "F") for age in AGE_GROUPS}
    results = [
        AgePopulationResult(org_unit_id="OU1", org_unit_name="District A", values=values),
        AgePopulationResult(org_unit_id="OU2", org_unit_name="District B", values=values),
    ]
    coc_mapping = {f"{sex}_{age}": f"COC_{sex}_{age}" for sex in ("M", "F") for age in AGE_GROUPS}

    dvs = build_data_values.fn(results, 2024, coc_mapping)

    assert len(dvs.dataValues) == 80  # 40 per org unit x 2


def test_import_to_dhis2() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE

    org_units = [OrgUnitGeo(id="ROOT_OU", name="Freetown", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])]
    dvs = Dhis2DataValueSet(
        dataValues=[
            DataValue(
                dataElement="PfAgPopEst1",
                period="2024",
                orgUnit="ROOT_OU",
                categoryOptionCombo=f"COC_M_{age}",
                value="100",
            )
            for age in AGE_GROUPS
        ]
    )

    result = import_to_dhis2.fn(mock_client, "https://dhis2.example.org", org_units, dvs)

    assert isinstance(result, ImportResult)
    assert result.imported == 40
    assert result.total == 20
    assert "Age-Sex" in result.markdown
    mock_client.post_data_values.assert_called_once()


def test_import_to_dhis2_empty() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    org_units = [OrgUnitGeo(id="ROOT_OU", name="Freetown", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])]

    result = import_to_dhis2.fn(mock_client, "https://dhis2.example.org", org_units, Dhis2DataValueSet())

    assert result.total == 0
    assert "No data values" in result.markdown
    mock_client.post_data_values.assert_not_called()


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("dhis2_worldpop_geotiff_age_import.zonal_population")
@patch("dhis2_worldpop_geotiff_age_import.download_tiff")
@patch.object(Dhis2Credentials, "get_client")
def test_flow_runs(
    mock_get_client: MagicMock,
    mock_download_tiff: MagicMock,
    mock_zonal: MagicMock,
) -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.side_effect = [
        SAMPLE_OU_WITH_GEOM,
        SAMPLE_COCS,
    ]
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE
    mock_get_client.return_value = mock_client

    # download_tiff returns a Path; mock stat for the print statement
    mock_path = MagicMock(spec=Path)
    mock_path.stat.return_value.st_size = 5_000_000
    mock_path.name = "test.tif"
    mock_download_tiff.return_value = mock_path
    mock_zonal.return_value = 500.0

    state = dhis2_worldpop_geotiff_age_import_flow(
        query=ImportQuery(iso3="SLE", years=[2024]),
        return_state=True,
    )
    assert state.is_completed()
    result = state.result()
    assert isinstance(result, ImportResult)
    assert result.imported == 40
    assert len(result.org_units) == 1
    assert result.org_units[0].id == "ROOT_OU"
    # 40 rasters downloaded individually
    assert mock_download_tiff.call_count == 40
