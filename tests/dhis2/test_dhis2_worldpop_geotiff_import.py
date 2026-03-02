"""Tests for DHIS2 WorldPop GeoTIFF Population Import flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

# ---------------------------------------------------------------------------
# Load the flow module via importlib (same pattern as existing tests)
# ---------------------------------------------------------------------------

_spec = importlib.util.spec_from_file_location(
    "dhis2_worldpop_geotiff_import",
    Path(__file__).resolve().parent.parent.parent / "flows" / "dhis2" / "dhis2_worldpop_geotiff_import.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["dhis2_worldpop_geotiff_import"] = _mod
_spec.loader.exec_module(_mod)

from prefect_dhis2 import (  # noqa: E402
    CocMapping,
    DataValue,
    Dhis2Client,
    Dhis2DataValueSet,
    MetadataResult,
    OrgUnitGeo,
)
from prefect_dhis2.credentials import Dhis2Credentials  # noqa: E402
from prefect_worldpop import ImportQuery, ImportResult, RasterPair, WorldPopResult  # noqa: E402
from prefect_worldpop.geotiff import build_tiff_url  # noqa: E402

ensure_dhis2_metadata = _mod.ensure_dhis2_metadata
download_worldpop_rasters = _mod.download_worldpop_rasters
compute_population = _mod.compute_population
build_data_values = _mod.build_data_values
import_to_dhis2 = _mod.import_to_dhis2
dhis2_worldpop_geotiff_import_flow = _mod.dhis2_worldpop_geotiff_import_flow

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

SAMPLE_OU_WITH_GEOM = [
    {
        "id": "ROOT_OU",
        "name": "Vientiane",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[102.0, 17.5], [103.0, 17.5], [103.0, 18.5], [102.0, 18.5], [102.0, 17.5]]],
        },
    }
]

SAMPLE_OU_POINT = [
    {
        "id": "POINT_OU",
        "name": "Point Org",
        "geometry": {"type": "Point", "coordinates": [102.0, 18.0]},
    }
]

SAMPLE_COCS = [
    {"id": "COC_MALE_1", "name": "Male", "categoryOptions": [{"id": "PfGtSexMal1"}]},
    {"id": "COC_FEML_1", "name": "Female", "categoryOptions": [{"id": "PfGtSexFem1"}]},
]

SAMPLE_METADATA_RESPONSE = {
    "status": "OK",
    "stats": {"created": 6, "updated": 0, "deleted": 0, "ignored": 0, "total": 6},
}

SAMPLE_IMPORT_RESPONSE = {
    "status": "SUCCESS",
    "importCount": {"imported": 2, "updated": 0, "ignored": 0, "deleted": 0},
}

EXPECTED_MALE_TOTAL = 15000.0
EXPECTED_FEMALE_TOTAL = 14500.0


# ---------------------------------------------------------------------------
# Library tests
# ---------------------------------------------------------------------------


def test_build_tiff_url() -> None:
    url = build_tiff_url("LAO", "M", 2024)
    assert url == (
        "https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/"
        "R2025A/2024/LAO/v1/100m/constrained/lao_T_M_2024_CN_100m_R2025A_v1.tif"
    )


def test_build_tiff_url_female() -> None:
    url = build_tiff_url("VNM", "F", 2020)
    assert url == (
        "https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/"
        "R2025A/2020/VNM/v1/100m/constrained/vnm_T_F_2020_CN_100m_R2025A_v1.tif"
    )


def test_build_tiff_url_case_handling() -> None:
    url = build_tiff_url("lao", "M", 2024)
    assert "/LAO/" in url
    assert "lao_T_M_" in url


@patch("prefect_worldpop.geotiff.rioxarray.open_rasterio")
def test_zonal_population(mock_open: MagicMock) -> None:
    from prefect_worldpop.geotiff import zonal_population

    mock_da = MagicMock()
    mock_clipped = MagicMock()
    mock_clipped.rio.nodata = -99999.0
    mock_masked = MagicMock()
    mock_masked.values = np.array([100.0, 200.0, 300.0])
    mock_clipped.where.return_value = mock_masked
    mock_da.rio.clip.return_value = mock_clipped
    mock_open.return_value = mock_da

    geometry = {
        "type": "Polygon",
        "coordinates": [[[102.0, 17.5], [103.0, 17.5], [103.0, 18.5], [102.0, 18.5], [102.0, 17.5]]],
    }
    result = zonal_population(Path("test.tif"), geometry)

    assert result == 600.0
    mock_da.rio.clip.assert_called_once_with([geometry], all_touched=True)
    mock_da.close.assert_called_once()


@patch("prefect_worldpop.geotiff.rioxarray.open_rasterio")
def test_zonal_population_no_nodata(mock_open: MagicMock) -> None:
    from prefect_worldpop.geotiff import zonal_population

    mock_da = MagicMock()
    mock_clipped = MagicMock()
    mock_clipped.rio.nodata = None
    mock_clipped.values = np.array([50.0, 150.0])
    mock_da.rio.clip.return_value = mock_clipped
    mock_open.return_value = mock_da

    geometry = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]}
    result = zonal_population(Path("test.tif"), geometry)

    assert result == 200.0
    mock_clipped.where.assert_not_called()


@patch("prefect_worldpop.geotiff.rioxarray.open_rasterio")
def test_population_by_sex(mock_open: MagicMock) -> None:
    from prefect_worldpop.geotiff import population_by_sex

    call_count = 0

    def _mock_open(path: Path) -> MagicMock:
        nonlocal call_count
        call_count += 1
        da = MagicMock()
        clipped = MagicMock()
        clipped.rio.nodata = None
        clipped.values = np.array([EXPECTED_MALE_TOTAL] if call_count == 1 else [EXPECTED_FEMALE_TOTAL])
        da.rio.clip.return_value = clipped
        return da

    mock_open.side_effect = _mock_open

    geometry = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]}
    male, female = population_by_sex(Path("male.tif"), Path("female.tif"), geometry)

    assert male == EXPECTED_MALE_TOTAL
    assert female == EXPECTED_FEMALE_TOTAL
    assert mock_open.call_count == 2


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_import_query_defaults() -> None:
    q = ImportQuery(iso3="LAO")
    assert q.iso3 == "LAO"
    assert q.org_unit_level == 2
    assert q.years == list(range(2020, 2026))


def test_import_query_custom() -> None:
    q = ImportQuery(iso3="VNM", org_unit_level=3, years=[2022, 2023])
    assert q.iso3 == "VNM"
    assert q.org_unit_level == 3
    assert q.years == [2022, 2023]


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

    assert isinstance(metadata, MetadataResult)
    assert len(metadata.org_units) == 1
    assert metadata.org_units[0].id == "ROOT_OU"
    assert metadata.org_units[0].name == "Vientiane"
    assert metadata.coc_mapping.male == "COC_MALE_1"
    assert metadata.coc_mapping.female == "COC_FEML_1"

    mock_client.post_metadata.assert_called_once()
    payload = mock_client.post_metadata.call_args[0][0]
    assert "categoryOptions" in payload
    assert "categories" in payload
    assert "categoryCombos" in payload
    assert "dataElements" in payload
    assert "dataSets" in payload
    assert len(payload["categoryOptions"]) == 2
    assert payload["categoryOptions"][0]["name"] == "GT: Male"
    assert payload["categoryOptions"][1]["name"] == "GT: Female"
    assert len(payload["categories"]) == 1
    assert len(payload["categoryCombos"]) == 1
    assert len(payload["dataElements"]) == 1
    assert payload["dataElements"][0]["name"] == "GT: WorldPop GeoTIFF Population"
    assert len(payload["dataSets"]) == 1
    assert payload["dataSets"][0]["organisationUnits"] == [{"id": "ROOT_OU"}]
    mock_client.run_maintenance.assert_called_once_with("categoryOptionComboUpdate")


def test_ensure_dhis2_metadata_level_3() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.side_effect = [
        SAMPLE_OU_WITH_GEOM,
        SAMPLE_COCS,
    ]
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE

    ensure_dhis2_metadata.fn(mock_client, 3)

    first_call = mock_client.fetch_metadata.call_args_list[0]
    assert first_call[1].get("filters") == ["level:eq:3"] or first_call.args == ("organisationUnits",)


def test_ensure_dhis2_metadata_no_polygon() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_OU_POINT

    with pytest.raises(ValueError, match="No level-2 org units with Polygon"):
        ensure_dhis2_metadata.fn(mock_client, 2)


@patch("dhis2_worldpop_geotiff_import.population_by_sex")
def test_compute_population(mock_pop: MagicMock) -> None:
    mock_pop.return_value = (EXPECTED_MALE_TOTAL, EXPECTED_FEMALE_TOTAL)

    ou = OrgUnitGeo(
        id="ROOT_OU",
        name="Vientiane",
        geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"],
    )
    rasters = RasterPair(male=Path("male.tif"), female=Path("female.tif"))
    result = compute_population.fn(ou, rasters)

    assert isinstance(result, WorldPopResult)
    assert result.org_unit_id == "ROOT_OU"
    assert result.org_unit_name == "Vientiane"
    assert result.male == EXPECTED_MALE_TOTAL
    assert result.female == EXPECTED_FEMALE_TOTAL


def test_build_data_values() -> None:
    results = [
        WorldPopResult(org_unit_id="ROOT_OU", org_unit_name="Vientiane", male=15000.0, female=14500.0),
    ]
    coc_mapping = CocMapping(male="COC_MALE_1", female="COC_FEML_1")

    dvs = build_data_values.fn(results, 2024, coc_mapping)

    assert isinstance(dvs, Dhis2DataValueSet)
    assert len(dvs.dataValues) == 2
    male_dv = [dv for dv in dvs.dataValues if dv.categoryOptionCombo == "COC_MALE_1"]
    female_dv = [dv for dv in dvs.dataValues if dv.categoryOptionCombo == "COC_FEML_1"]
    assert len(male_dv) == 1
    assert len(female_dv) == 1
    assert male_dv[0].dataElement == "PfGtPopEst1"
    assert male_dv[0].orgUnit == "ROOT_OU"
    assert male_dv[0].period == "2024"
    assert male_dv[0].value == "15000"
    assert female_dv[0].value == "14500"


def test_import_to_dhis2() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE

    org_units = [OrgUnitGeo(id="ROOT_OU", name="Vientiane", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])]
    dvs = Dhis2DataValueSet(
        dataValues=[
            DataValue(
                dataElement="PfGtPopEst1",
                period="2024",
                orgUnit="ROOT_OU",
                categoryOptionCombo="COC_MALE_1",
                value="15000",
            ),
            DataValue(
                dataElement="PfGtPopEst1",
                period="2024",
                orgUnit="ROOT_OU",
                categoryOptionCombo="COC_FEML_1",
                value="14500",
            ),
        ]
    )

    result = import_to_dhis2.fn(mock_client, "https://dhis2.example.org", org_units, dvs)

    assert isinstance(result, ImportResult)
    assert result.imported == 2
    assert result.total == 2
    assert "GeoTIFF" in result.markdown
    mock_client.post_data_values.assert_called_once()
    payload = mock_client.post_data_values.call_args[0][0]
    assert len(payload["dataValues"]) == 2


def test_import_to_dhis2_empty() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    org_units = [OrgUnitGeo(id="ROOT_OU", name="Vientiane", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])]

    result = import_to_dhis2.fn(mock_client, "https://dhis2.example.org", org_units, Dhis2DataValueSet())

    assert result.total == 0
    assert "No data values" in result.markdown
    mock_client.post_data_values.assert_not_called()


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("dhis2_worldpop_geotiff_import.population_by_sex")
@patch("dhis2_worldpop_geotiff_import.download_sex_rasters")
@patch.object(Dhis2Credentials, "get_client")
def test_flow_runs(
    mock_get_client: MagicMock,
    mock_download: MagicMock,
    mock_pop: MagicMock,
) -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.side_effect = [
        SAMPLE_OU_WITH_GEOM,
        SAMPLE_COCS,
    ]
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE
    mock_get_client.return_value = mock_client

    mock_download.return_value = (Path("male.tif"), Path("female.tif"))
    mock_pop.return_value = (EXPECTED_MALE_TOTAL, EXPECTED_FEMALE_TOTAL)

    state = dhis2_worldpop_geotiff_import_flow(
        query=ImportQuery(iso3="LAO", years=[2024]),
        return_state=True,
    )
    assert state.is_completed()
    result = state.result()
    assert isinstance(result, ImportResult)
    assert result.imported == 2
    assert len(result.org_units) == 1
    assert result.org_units[0].id == "ROOT_OU"
