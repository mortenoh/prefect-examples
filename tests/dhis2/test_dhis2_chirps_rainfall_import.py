"""Tests for DHIS2 CHIRPS Rainfall Import flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Load the flow module via importlib (same pattern as existing tests)
# ---------------------------------------------------------------------------

_spec = importlib.util.spec_from_file_location(
    "dhis2_chirps_rainfall_import",
    Path(__file__).resolve().parent.parent.parent / "flows" / "dhis2" / "dhis2_chirps_rainfall_import.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["dhis2_chirps_rainfall_import"] = _mod
_spec.loader.exec_module(_mod)

from prefect_climate import ClimateQuery, ClimateResult, ImportResult  # noqa: E402
from prefect_climate.chirps import build_chirps_url  # noqa: E402
from prefect_dhis2 import (  # noqa: E402
    DataValue,
    Dhis2Client,
    Dhis2DataValueSet,
    OrgUnitGeo,
)
from prefect_dhis2.credentials import Dhis2Credentials  # noqa: E402

ensure_dhis2_metadata = _mod.ensure_dhis2_metadata
download_chirps_raster = _mod.download_chirps_raster
compute_rainfall = _mod.compute_rainfall
build_data_values = _mod.build_data_values
import_to_dhis2 = _mod.import_to_dhis2
dhis2_chirps_rainfall_import_flow = _mod.dhis2_chirps_rainfall_import_flow

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

SAMPLE_METADATA_RESPONSE = {
    "status": "OK",
    "stats": {"created": 2, "updated": 0, "deleted": 0, "ignored": 0, "total": 2},
}

SAMPLE_IMPORT_RESPONSE = {
    "status": "SUCCESS",
    "importCount": {"imported": 12, "updated": 0, "ignored": 0, "deleted": 0},
}


# ---------------------------------------------------------------------------
# Library tests
# ---------------------------------------------------------------------------


def test_build_chirps_url() -> None:
    url = build_chirps_url(2024, 1)
    assert url == "https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_monthly/tifs/chirps-v2.0.2024.01.tif.gz"


def test_build_chirps_url_december() -> None:
    url = build_chirps_url(2023, 12)
    assert url == "https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_monthly/tifs/chirps-v2.0.2023.12.tif.gz"


def test_download_chirps_caching(tmp_path: Path) -> None:
    """Test that download_chirps returns cached file when it exists."""
    from prefect_climate.chirps import download_chirps

    cached = tmp_path / "chirps-v2.0.2024.01.tif"
    cached.write_bytes(b"fake tiff data")

    url = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_monthly/tifs/chirps-v2.0.2024.01.tif.gz"
    result = download_chirps(url, tmp_path)

    assert result == cached
    assert result.exists()


# ---------------------------------------------------------------------------
# Task tests
# ---------------------------------------------------------------------------


def test_ensure_dhis2_metadata() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_OU_WITH_GEOM
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE

    org_units = ensure_dhis2_metadata.fn(mock_client, 2)

    assert len(org_units) == 1
    assert org_units[0].id == "ROOT_OU"
    assert org_units[0].name == "Freetown"

    mock_client.post_metadata.assert_called_once()
    payload = mock_client.post_metadata.call_args[0][0]
    assert "dataElements" in payload
    assert "dataSets" in payload
    # No category combos for climate data
    assert payload.get("categoryOptions", []) == []
    assert payload.get("categories", []) == []
    assert payload.get("categoryCombos", []) == []
    assert len(payload["dataElements"]) == 1
    assert payload["dataElements"][0]["name"] == "PR: CHIRPS: Rainfall"
    assert payload["dataSets"][0]["periodType"] == "Monthly"


def test_ensure_dhis2_metadata_no_polygon() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_OU_POINT

    with pytest.raises(ValueError, match="No level-2 org units with Polygon"):
        ensure_dhis2_metadata.fn(mock_client, 2)


@patch("dhis2_chirps_rainfall_import.zonal_mean")
def test_compute_rainfall(mock_zonal: MagicMock) -> None:
    mock_zonal.return_value = 150.5

    ou = OrgUnitGeo(
        id="ROOT_OU",
        name="Freetown",
        geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"],
    )
    monthly_rasters = {1: Path("jan.tif"), 2: Path("feb.tif"), 3: Path("mar.tif")}
    result = compute_rainfall.fn(ou, monthly_rasters)

    assert isinstance(result, ClimateResult)
    assert result.org_unit_id == "ROOT_OU"
    assert result.org_unit_name == "Freetown"
    assert len(result.monthly_values) == 3
    assert all(v == 150.5 for v in result.monthly_values.values())
    assert mock_zonal.call_count == 3


def test_build_data_values() -> None:
    results = [
        ClimateResult(
            org_unit_id="ROOT_OU",
            org_unit_name="Freetown",
            monthly_values={1: 120.5, 2: 85.3, 3: 200.1},
        ),
    ]
    dvs = build_data_values.fn(results, 2024)

    assert isinstance(dvs, Dhis2DataValueSet)
    assert len(dvs.dataValues) == 3
    periods = {dv.period for dv in dvs.dataValues}
    assert periods == {"202401", "202402", "202403"}
    for dv in dvs.dataValues:
        assert dv.dataElement == "PfChRnfEst1"
        assert dv.orgUnit == "ROOT_OU"


def test_build_data_values_multiple_org_units() -> None:
    results = [
        ClimateResult(
            org_unit_id="OU1",
            org_unit_name="District A",
            monthly_values={m: 100.0 + m * 10 for m in range(1, 13)},
        ),
        ClimateResult(
            org_unit_id="OU2",
            org_unit_name="District B",
            monthly_values={m: 50.0 + m * 5 for m in range(1, 13)},
        ),
    ]
    dvs = build_data_values.fn(results, 2024)
    assert len(dvs.dataValues) == 24  # 12 months x 2 org units


def test_import_to_dhis2() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE

    org_units = [OrgUnitGeo(id="ROOT_OU", name="Freetown", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])]
    dvs = Dhis2DataValueSet(
        dataValues=[
            DataValue(
                dataElement="PfChRnfEst1",
                period=f"2024{m:02d}",
                orgUnit="ROOT_OU",
                categoryOptionCombo="",
                value=str(100.0 + m * 10),
            )
            for m in range(1, 13)
        ]
    )

    result = import_to_dhis2.fn(mock_client, "https://dhis2.example.org", org_units, dvs)

    assert isinstance(result, ImportResult)
    assert result.imported == 12
    assert result.total == 12
    assert "CHIRPS" in result.markdown
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


@patch("dhis2_chirps_rainfall_import.zonal_mean")
@patch("dhis2_chirps_rainfall_import.download_chirps")
@patch.object(Dhis2Credentials, "get_client")
def test_flow_runs(
    mock_get_client: MagicMock,
    mock_download: MagicMock,
    mock_zonal: MagicMock,
) -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_OU_WITH_GEOM
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE
    mock_get_client.return_value = mock_client

    mock_download.return_value = Path("chirps.tif")
    mock_zonal.return_value = 150.0

    state = dhis2_chirps_rainfall_import_flow(
        query=ClimateQuery(iso3="SLE", year=2024, months=[1, 2, 3]),
        return_state=True,
    )
    assert state.is_completed()
    result = state.result()
    assert isinstance(result, ImportResult)
    assert result.imported == 12
    assert len(result.org_units) == 1
    assert result.org_units[0].id == "ROOT_OU"
