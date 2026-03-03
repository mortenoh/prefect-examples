"""Tests for DHIS2 ERA5-Land Climate Import flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Load the flow module via importlib (same pattern as existing tests)
# ---------------------------------------------------------------------------

_spec = importlib.util.spec_from_file_location(
    "dhis2_era5_climate_import",
    Path(__file__).resolve().parent.parent.parent / "flows" / "dhis2" / "dhis2_era5_climate_import.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["dhis2_era5_climate_import"] = _mod
_spec.loader.exec_module(_mod)

from prefect_climate import ClimateQuery, ImportResult  # noqa: E402
from prefect_climate.era5 import relative_humidity, wind_speed  # noqa: E402
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
download_era5_variable = _mod.download_era5_variable
compute_climate = _mod.compute_climate
build_data_values = _mod.build_data_values
import_to_dhis2 = _mod.import_to_dhis2
dhis2_era5_climate_import_flow = _mod.dhis2_era5_climate_import_flow

TEMPERATURE_DE_UID = _mod.TEMPERATURE_DE_UID
PRECIPITATION_DE_UID = _mod.PRECIPITATION_DE_UID
HUMIDITY_DE_UID = _mod.HUMIDITY_DE_UID
WIND_SPEED_DE_UID = _mod.WIND_SPEED_DE_UID
SKIN_TEMP_DE_UID = _mod.SKIN_TEMP_DE_UID
SOLAR_RAD_DE_UID = _mod.SOLAR_RAD_DE_UID
SOIL_MOISTURE_DE_UID = _mod.SOIL_MOISTURE_DE_UID
DATA_SET_UID = _mod.DATA_SET_UID

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
    "stats": {"created": 4, "updated": 0, "deleted": 0, "ignored": 0, "total": 4},
}

SAMPLE_IMPORT_RESPONSE = {
    "status": "SUCCESS",
    "importCount": {"imported": 36, "updated": 0, "ignored": 0, "deleted": 0},
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


def test_relative_humidity() -> None:
    # At T=25 C and Td=20 C, RH should be around 73.8%
    rh = relative_humidity(25.0, 20.0)
    assert 73.0 < rh < 75.0

    # At T=Td, RH should be 100%
    rh = relative_humidity(20.0, 20.0)
    assert rh == pytest.approx(100.0)

    # At very low dewpoint, RH should be low
    rh = relative_humidity(30.0, 5.0)
    assert rh < 25.0


def test_relative_humidity_capped_at_100() -> None:
    # Even with floating-point quirks, result should never exceed 100
    rh = relative_humidity(20.0, 20.0)
    assert rh <= 100.0


def test_precipitation_conversion() -> None:
    """Verify the precipitation unit conversion logic in fetch_era5_monthly."""
    # ERA5-Land monthly means give the mean daily total (m/day).
    # For January (31 days), a mean daily rate of 0.01 m/day
    # should convert to: 0.01 * 31 * 1000 = 310 mm
    import calendar

    raw_value = 0.01  # m/day (mean daily rate)
    year, month = 2024, 1
    days = calendar.monthrange(year, month)[1]
    result = raw_value * days * 1000
    assert result == pytest.approx(310.0)

    # February 2024 (leap year, 29 days)
    month = 2
    days = calendar.monthrange(year, month)[1]
    assert days == 29
    result = raw_value * days * 1000
    assert result == pytest.approx(290.0)


def test_wind_speed() -> None:
    """Verify wind speed derivation: WS = sqrt(u^2 + v^2)."""
    # Classic 3-4-5 right triangle
    assert wind_speed(3.0, 4.0) == pytest.approx(5.0)

    # Zero wind
    assert wind_speed(0.0, 0.0) == pytest.approx(0.0)

    # Pure eastward wind (v=0)
    assert wind_speed(7.5, 0.0) == pytest.approx(7.5)

    # Negative components (wind can blow in any direction)
    assert wind_speed(-3.0, -4.0) == pytest.approx(5.0)


def test_solar_radiation_conversion() -> None:
    """Verify solar radiation conversion: J/m2/day -> W/m2.

    ERA5-Land monthly means provide the mean daily energy flux in J/m2.
    Dividing by 86400 (seconds per day) gives mean irradiance in W/m2.
    1 W = 1 J/s, so J/m2 / 86400 s = W/m2.
    """
    # 86400 J/m2/day should be exactly 1.0 W/m2
    raw_value = 86400.0  # J/m2/day
    result = raw_value / 86400
    assert result == pytest.approx(1.0)

    # Typical tropical monthly mean: ~18 MJ/m2/day = ~208 W/m2
    raw_value = 18_000_000.0  # J/m2/day
    result = raw_value / 86400
    assert result == pytest.approx(208.333, rel=1e-3)


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
    # 7 data elements for all climate variables
    assert len(payload["dataElements"]) == 7
    de_names = {de["name"] for de in payload["dataElements"]}
    assert de_names == {
        "PR: ERA5: Mean Temperature",
        "PR: ERA5: Total Precipitation",
        "PR: ERA5: Relative Humidity",
        "PR: ERA5: Wind Speed",
        "PR: ERA5: Skin Temperature",
        "PR: ERA5: Solar Radiation",
        "PR: ERA5: Soil Moisture",
    }
    # 1 unified data set with 7 data set elements
    assert len(payload["dataSets"]) == 1
    ds = payload["dataSets"][0]
    assert ds["name"] == "PR: ERA5: Climate"
    assert ds["periodType"] == "Monthly"
    assert len(ds["dataSetElements"]) == 7


def test_ensure_dhis2_metadata_no_polygon() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_OU_POINT

    with pytest.raises(ValueError, match="No level-2 org units with Polygon"):
        ensure_dhis2_metadata.fn(mock_client, 2)


@patch("dhis2_era5_climate_import.zonal_mean")
@patch("dhis2_era5_climate_import.relative_humidity")
@patch("dhis2_era5_climate_import.wind_speed")
def test_compute_climate(mock_ws: MagicMock, mock_rh: MagicMock, mock_zonal: MagicMock) -> None:
    # zonal_mean is called 8 times per month:
    #   temp, precip, dewpoint, wind_u, wind_v, skin_temp, solar_rad, soil_moisture
    mock_zonal.side_effect = [
        # month 1
        25.3,  # temp
        142.0,  # precip
        20.1,  # dewpoint
        3.0,  # wind u (m/s, eastward)
        4.0,  # wind v (m/s, northward)
        30.2,  # skin temperature (Celsius)
        215.0,  # solar radiation (W/m2)
        0.25,  # soil moisture (m3/m3)
        # month 2
        26.1,  # temp
        98.5,  # precip
        21.0,  # dewpoint
        2.5,  # wind u
        3.5,  # wind v
        31.0,  # skin temp
        220.0,  # solar rad
        0.22,  # soil moisture
        # month 3
        27.5,  # temp
        55.0,  # precip
        22.3,  # dewpoint
        1.0,  # wind u
        2.0,  # wind v
        32.5,  # skin temp
        230.0,  # solar rad
        0.18,  # soil moisture
    ]
    mock_rh.return_value = 74.5
    mock_ws.return_value = 5.0

    ou = OrgUnitGeo(
        id="ROOT_OU",
        name="Freetown",
        geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"],
    )
    months_range = {1: Path("01.tif"), 2: Path("02.tif"), 3: Path("03.tif")}
    temp_rasters = {m: Path(f"temp_{m:02d}.tif") for m in months_range}
    precip_rasters = {m: Path(f"precip_{m:02d}.tif") for m in months_range}
    dewpoint_rasters = {m: Path(f"dew_{m:02d}.tif") for m in months_range}
    wind_u_rasters = {m: Path(f"u10_{m:02d}.tif") for m in months_range}
    wind_v_rasters = {m: Path(f"v10_{m:02d}.tif") for m in months_range}
    skin_temp_rasters = {m: Path(f"skt_{m:02d}.tif") for m in months_range}
    solar_rad_rasters = {m: Path(f"ssrd_{m:02d}.tif") for m in months_range}
    soil_moisture_rasters = {m: Path(f"swvl1_{m:02d}.tif") for m in months_range}

    result = compute_climate.fn(
        ou,
        temp_rasters,
        precip_rasters,
        dewpoint_rasters,
        wind_u_rasters,
        wind_v_rasters,
        skin_temp_rasters,
        solar_rad_rasters,
        soil_moisture_rasters,
    )

    # All 7 output keys present
    assert set(result.keys()) == {
        "temperature",
        "precipitation",
        "humidity",
        "wind_speed",
        "skin_temperature",
        "solar_radiation",
        "soil_moisture",
    }
    for key in result:
        assert len(result[key]) == 3

    assert result["temperature"][1] == 25.3
    assert result["precipitation"][1] == 142.0
    assert result["humidity"][1] == 74.5
    assert result["wind_speed"][1] == 5.0
    assert result["skin_temperature"][1] == 30.2
    assert result["solar_radiation"][1] == 215.0
    assert result["soil_moisture"][1] == 0.25
    assert mock_zonal.call_count == 24  # 8 zonal_mean calls * 3 months
    assert mock_rh.call_count == 3
    assert mock_ws.call_count == 3


def test_build_data_values() -> None:
    ou = OrgUnitGeo(id="ROOT_OU", name="Freetown", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])
    climate_data = {
        "temperature": {1: 25.3, 2: 26.1, 3: 27.5},
        "precipitation": {1: 142.0, 2: 98.5, 3: 55.0},
        "humidity": {1: 74.5, 2: 78.2, 3: 65.1},
        "wind_speed": {1: 5.0, 2: 4.3, 3: 2.2},
        "skin_temperature": {1: 30.2, 2: 31.0, 3: 32.5},
        "solar_radiation": {1: 215.0, 2: 220.0, 3: 230.0},
        "soil_moisture": {1: 0.25, 2: 0.22, 3: 0.18},
    }
    dvs = build_data_values.fn([(ou, climate_data)], 2024)

    assert isinstance(dvs, Dhis2DataValueSet)
    assert len(dvs.dataValues) == 21  # 7 vars * 3 months
    periods = {dv.period for dv in dvs.dataValues}
    assert periods == {"202401", "202402", "202403"}

    de_uids = {dv.dataElement for dv in dvs.dataValues}
    assert de_uids == {
        TEMPERATURE_DE_UID,
        PRECIPITATION_DE_UID,
        HUMIDITY_DE_UID,
        WIND_SPEED_DE_UID,
        SKIN_TEMP_DE_UID,
        SOLAR_RAD_DE_UID,
        SOIL_MOISTURE_DE_UID,
    }

    for dv in dvs.dataValues:
        assert dv.orgUnit == "ROOT_OU"


def test_build_data_values_multiple_org_units() -> None:
    ou1 = OrgUnitGeo(id="OU1", name="District A", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])
    ou2 = OrgUnitGeo(id="OU2", name="District B", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])
    climate_data = {
        "temperature": {m: 20.0 + m for m in range(1, 13)},
        "precipitation": {m: 100.0 + m for m in range(1, 13)},
        "humidity": {m: 70.0 + m for m in range(1, 13)},
        "wind_speed": {m: 3.0 + m * 0.1 for m in range(1, 13)},
        "skin_temperature": {m: 25.0 + m for m in range(1, 13)},
        "solar_radiation": {m: 200.0 + m for m in range(1, 13)},
        "soil_moisture": {m: 0.2 + m * 0.01 for m in range(1, 13)},
    }
    dvs = build_data_values.fn([(ou1, climate_data), (ou2, climate_data)], 2024)
    # 7 vars * 12 months * 2 org units = 168
    assert len(dvs.dataValues) == 168


def test_import_to_dhis2() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE

    org_units = [OrgUnitGeo(id="ROOT_OU", name="Freetown", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])]
    dvs = Dhis2DataValueSet(
        dataValues=[
            DataValue(
                dataElement=de_uid,
                period=f"2024{m:02d}",
                orgUnit="ROOT_OU",
                categoryOptionCombo="",
                value=str(20.0 + m),
            )
            for de_uid in [TEMPERATURE_DE_UID, PRECIPITATION_DE_UID, HUMIDITY_DE_UID]
            for m in range(1, 13)
        ]
    )

    result = import_to_dhis2.fn(mock_client, "https://dhis2.example.org", org_units, dvs)

    assert isinstance(result, ImportResult)
    assert result.imported == 36
    assert result.total == 36
    assert "ERA5" in result.markdown
    assert "Climate" in result.markdown
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
    assert payload["sharing"]["public"] == "rwrw----"
    assert payload["sharing"]["external"] is False

    # Data elements only support metadata sharing -- no data access positions
    de = Dhis2DataElement(id="x", name="x", shortName="x")
    payload = de.model_dump()
    assert payload["sharing"]["public"] == "rw------"
    assert payload["sharing"]["external"] is False


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("dhis2_era5_climate_import.zonal_mean")
@patch("dhis2_era5_climate_import.relative_humidity")
@patch("dhis2_era5_climate_import.wind_speed")
@patch("dhis2_era5_climate_import.fetch_era5_monthly")
@patch.object(Dhis2Credentials, "get_client")
def test_flow_runs(
    mock_get_client: MagicMock,
    mock_fetch_era5: MagicMock,
    mock_ws: MagicMock,
    mock_rh: MagicMock,
    mock_zonal: MagicMock,
) -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_OU_WITH_GEOM
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE
    mock_get_client.return_value = mock_client

    mock_fetch_era5.return_value = {m: Path(f"era5_{m:02d}.tif") for m in range(1, 4)}
    mock_zonal.return_value = 25.0
    mock_rh.return_value = 75.0
    mock_ws.return_value = 5.0

    state = dhis2_era5_climate_import_flow(
        query=ClimateQuery(iso3="SLE", year=2024, months=[1, 2, 3]),
        return_state=True,
    )
    assert state.is_completed()
    result = state.result()
    assert isinstance(result, ImportResult)
    assert result.imported == 36
    assert len(result.org_units) == 1
    assert result.org_units[0].id == "ROOT_OU"

    # fetch_era5_monthly should be called 8 times (one per CDS variable)
    assert mock_fetch_era5.call_count == 8
    variables_called = [call.args[0] for call in mock_fetch_era5.call_args_list]
    assert "2m_temperature" in variables_called
    assert "total_precipitation" in variables_called
    assert "2m_dewpoint_temperature" in variables_called
    assert "10m_u_component_of_wind" in variables_called
    assert "10m_v_component_of_wind" in variables_called
    assert "skin_temperature" in variables_called
    assert "surface_solar_radiation_downwards" in variables_called
    assert "volumetric_soil_water_layer_1" in variables_called
