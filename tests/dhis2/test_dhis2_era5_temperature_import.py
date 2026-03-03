"""Tests for DHIS2 ERA5-Land Temperature Import flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Load the flow module via importlib (same pattern as existing tests)
# ---------------------------------------------------------------------------

_spec = importlib.util.spec_from_file_location(
    "dhis2_era5_temperature_import",
    Path(__file__).resolve().parent.parent.parent / "flows" / "dhis2" / "dhis2_era5_temperature_import.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["dhis2_era5_temperature_import"] = _mod
_spec.loader.exec_module(_mod)

from prefect_climate import ClimateQuery, ClimateResult, ImportResult  # noqa: E402
from prefect_climate.zonal import bounding_box  # noqa: E402
from prefect_dhis2 import (  # noqa: E402
    DataValue,
    Dhis2Client,
    Dhis2DataElement,
    Dhis2DataSet,
    Dhis2DataValueSet,
    OrgUnitGeo,
)
from prefect_dhis2.credentials import Dhis2Credentials  # noqa: E402

ensure_dhis2_metadata = _mod.ensure_dhis2_metadata
download_era5_temperature = _mod.download_era5_temperature
compute_temperature = _mod.compute_temperature
build_data_values = _mod.build_data_values
import_to_dhis2 = _mod.import_to_dhis2
dhis2_era5_temperature_import_flow = _mod.dhis2_era5_temperature_import_flow

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


def test_bounding_box() -> None:
    org_units = [
        OrgUnitGeo(
            id="OU1",
            name="District A",
            geometry={
                "type": "Polygon",
                "coordinates": [[[10.0, 5.0], [11.0, 5.0], [11.0, 6.0], [10.0, 6.0], [10.0, 5.0]]],
            },
        ),
        OrgUnitGeo(
            id="OU2",
            name="District B",
            geometry={
                "type": "Polygon",
                "coordinates": [[[12.0, 4.0], [13.0, 4.0], [13.0, 7.0], [12.0, 7.0], [12.0, 4.0]]],
            },
        ),
    ]
    bbox = bounding_box(org_units)
    # [N, W, S, E] with 0.1 buffer
    assert bbox[0] == pytest.approx(7.1)  # north
    assert bbox[1] == pytest.approx(9.9)  # west
    assert bbox[2] == pytest.approx(3.9)  # south
    assert bbox[3] == pytest.approx(13.1)  # east


def test_bounding_box_empty() -> None:
    with pytest.raises(ValueError, match="No coordinates"):
        bounding_box([])


@patch("prefect_climate.zonal.rioxarray.open_rasterio")
def test_zonal_mean(mock_open: MagicMock) -> None:
    import numpy as np
    from prefect_climate.zonal import zonal_mean

    mock_da = MagicMock()
    mock_clipped = MagicMock()
    mock_clipped.rio.nodata = -99999.0
    mock_masked = MagicMock()
    mock_masked.values = np.array([20.0, 25.0, 30.0])
    mock_clipped.where.return_value = mock_masked
    mock_da.rio.clip.return_value = mock_clipped
    mock_open.return_value = mock_da

    geometry = {
        "type": "Polygon",
        "coordinates": [[[-13.3, 8.4], [-13.2, 8.4], [-13.2, 8.5], [-13.3, 8.5], [-13.3, 8.4]]],
    }
    result = zonal_mean(Path("test.tif"), geometry)

    assert result == 25.0  # mean of [20, 25, 30]
    mock_da.rio.clip.assert_called_once_with([geometry], all_touched=True)
    mock_da.close.assert_called_once()


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
    assert payload["dataElements"][0]["name"] == "PR: ERA5: Mean Temperature"
    assert payload["dataSets"][0]["periodType"] == "Monthly"


def test_ensure_dhis2_metadata_no_polygon() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_OU_POINT

    with pytest.raises(ValueError, match="No level-2 org units with Polygon"):
        ensure_dhis2_metadata.fn(mock_client, 2)


@patch("dhis2_era5_temperature_import.zonal_mean")
def test_compute_temperature(mock_zonal: MagicMock) -> None:
    mock_zonal.return_value = 25.3

    ou = OrgUnitGeo(
        id="ROOT_OU",
        name="Freetown",
        geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"],
    )
    monthly_rasters = {1: Path("jan.tif"), 2: Path("feb.tif"), 3: Path("mar.tif")}
    result = compute_temperature.fn(ou, monthly_rasters)

    assert isinstance(result, ClimateResult)
    assert result.org_unit_id == "ROOT_OU"
    assert result.org_unit_name == "Freetown"
    assert len(result.monthly_values) == 3
    assert all(v == 25.3 for v in result.monthly_values.values())
    assert mock_zonal.call_count == 3


def test_build_data_values() -> None:
    results = [
        ClimateResult(
            org_unit_id="ROOT_OU",
            org_unit_name="Freetown",
            monthly_values={1: 25.3, 2: 26.1, 3: 27.5},
        ),
    ]
    dvs = build_data_values.fn(results, 2024)

    assert isinstance(dvs, Dhis2DataValueSet)
    assert len(dvs.dataValues) == 3
    periods = {dv.period for dv in dvs.dataValues}
    assert periods == {"202401", "202402", "202403"}
    for dv in dvs.dataValues:
        assert dv.dataElement == "PfE5TmpEst1"
        assert dv.orgUnit == "ROOT_OU"


def test_build_data_values_multiple_org_units() -> None:
    results = [
        ClimateResult(
            org_unit_id="OU1",
            org_unit_name="District A",
            monthly_values={m: 20.0 + m for m in range(1, 13)},
        ),
        ClimateResult(
            org_unit_id="OU2",
            org_unit_name="District B",
            monthly_values={m: 22.0 + m for m in range(1, 13)},
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
                dataElement="PfE5TmpEst1",
                period=f"2024{m:02d}",
                orgUnit="ROOT_OU",
                categoryOptionCombo="",
                value=str(20.0 + m),
            )
            for m in range(1, 13)
        ]
    )

    result = import_to_dhis2.fn(mock_client, "https://dhis2.example.org", org_units, dvs)

    assert isinstance(result, ImportResult)
    assert result.imported == 12
    assert result.total == 12
    assert "ERA5" in result.markdown
    mock_client.post_data_values.assert_called_once()


def test_import_to_dhis2_empty() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    org_units = [OrgUnitGeo(id="ROOT_OU", name="Freetown", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])]

    result = import_to_dhis2.fn(mock_client, "https://dhis2.example.org", org_units, Dhis2DataValueSet())

    assert result.total == 0
    assert "No data values" in result.markdown
    mock_client.post_data_values.assert_not_called()


def test_sharing_defaults() -> None:
    # Data sets support data sharing -- default is metadata rw + data read-only
    ds = Dhis2DataSet(id="x", name="x", shortName="x")
    payload = ds.model_dump()
    assert payload["sharing"]["public"] == "rwr-----"
    assert payload["sharing"]["external"] is False

    # Data elements only support metadata sharing -- no data access positions
    de = Dhis2DataElement(id="x", name="x", shortName="x")
    payload = de.model_dump()
    assert payload["sharing"]["public"] == "rw------"
    assert payload["sharing"]["external"] is False


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("dhis2_era5_temperature_import.zonal_mean")
@patch("dhis2_era5_temperature_import.fetch_era5_monthly")
@patch.object(Dhis2Credentials, "get_client")
def test_flow_runs(
    mock_get_client: MagicMock,
    mock_fetch_era5: MagicMock,
    mock_zonal: MagicMock,
) -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_OU_WITH_GEOM
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE
    mock_get_client.return_value = mock_client

    mock_fetch_era5.return_value = {m: Path(f"era5_{m:02d}.tif") for m in range(1, 4)}
    mock_zonal.return_value = 25.0

    state = dhis2_era5_temperature_import_flow(
        query=ClimateQuery(iso3="SLE", year=2024, months=[1, 2, 3]),
        return_state=True,
    )
    assert state.is_completed()
    result = state.result()
    assert isinstance(result, ImportResult)
    assert result.imported == 12
    assert len(result.org_units) == 1
    assert result.org_units[0].id == "ROOT_OU"
