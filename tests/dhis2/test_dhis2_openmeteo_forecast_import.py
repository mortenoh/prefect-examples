"""Tests for DHIS2 Open-Meteo Weather Forecast Import flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Load the flow module via importlib
# ---------------------------------------------------------------------------

_spec = importlib.util.spec_from_file_location(
    "dhis2_openmeteo_forecast_import",
    Path(__file__).resolve().parent.parent.parent / "flows" / "dhis2" / "dhis2_openmeteo_forecast_import.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["dhis2_openmeteo_forecast_import"] = _mod
_spec.loader.exec_module(_mod)

from prefect_climate import ClimateQuery, ImportResult  # noqa: E402
from prefect_climate.openmeteo import DailyWeather  # noqa: E402
from prefect_climate.openmeteo.historical import _aggregate_weather_daily  # noqa: E402
from prefect_dhis2 import (  # noqa: E402
    DataValue,
    Dhis2Client,
    Dhis2DataValueSet,
    OrgUnitGeo,
)
from prefect_dhis2.credentials import Dhis2Credentials  # noqa: E402

ensure_dhis2_metadata = _mod.ensure_dhis2_metadata
fetch_org_unit_forecast = _mod.fetch_org_unit_forecast
build_data_values = _mod.build_data_values
import_to_dhis2 = _mod.import_to_dhis2
dhis2_openmeteo_forecast_import_flow = _mod.dhis2_openmeteo_forecast_import_flow

TEMPERATURE_DE_UID = _mod.TEMPERATURE_DE_UID
PRECIPITATION_DE_UID = _mod.PRECIPITATION_DE_UID
HUMIDITY_DE_UID = _mod.HUMIDITY_DE_UID
WIND_SPEED_DE_UID = _mod.WIND_SPEED_DE_UID
CLOUD_COVER_DE_UID = _mod.CLOUD_COVER_DE_UID
PRESSURE_DE_UID = _mod.PRESSURE_DE_UID
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
    "stats": {"created": 7, "updated": 0, "deleted": 0, "ignored": 0, "total": 7},
}

SAMPLE_IMPORT_RESPONSE = {
    "status": "SUCCESS",
    "importCount": {"imported": 12, "updated": 0, "ignored": 0, "deleted": 0},
}

# Minimal Open-Meteo forecast response with hourly entries across 2 days.
SAMPLE_OPENMETEO_RESPONSE = {
    "hourly": {
        "time": [
            "2024-01-15T06:00",
            "2024-01-15T12:00",
            "2024-01-16T06:00",
        ],
        "temperature_2m": [26.0, 30.0, 25.0],
        "precipitation": [0.5, 0.0, 5.0],
        "relative_humidity_2m": [80.0, 60.0, 85.0],
        "wind_speed_10m": [3.5, 4.5, 2.0],
        "cloud_cover": [40.0, 20.0, 90.0],
        "pressure_msl": [1012.0, 1010.0, 1008.0],
    }
}


# ---------------------------------------------------------------------------
# Aggregation tests
# ---------------------------------------------------------------------------


def test_aggregate_weather_daily() -> None:
    forecasts = _aggregate_weather_daily(SAMPLE_OPENMETEO_RESPONSE)

    assert len(forecasts) == 2
    assert forecasts[0].date == "2024-01-15"
    assert forecasts[1].date == "2024-01-16"

    # Day 1: mean of [26.0, 30.0] = 28.0
    assert forecasts[0].temperature == pytest.approx(28.0)
    # Day 1: sum of [0.5, 0.0] = 0.5
    assert forecasts[0].precipitation == pytest.approx(0.5)
    # Day 1: mean of [80.0, 60.0] = 70.0
    assert forecasts[0].humidity == pytest.approx(70.0)
    # Day 1: mean of [3.5, 4.5] = 4.0
    assert forecasts[0].wind_speed == pytest.approx(4.0)
    # Day 1: mean of [40.0, 20.0] = 30.0
    assert forecasts[0].cloud_cover == pytest.approx(30.0)
    # Day 1: mean of [1012.0, 1010.0] = 1011.0
    assert forecasts[0].pressure == pytest.approx(1011.0)

    # Day 2: single entry
    assert forecasts[1].temperature == pytest.approx(25.0)
    assert forecasts[1].precipitation == pytest.approx(5.0)


def test_aggregate_weather_daily_empty() -> None:
    forecasts = _aggregate_weather_daily({"hourly": {"time": []}})
    assert forecasts == []


def test_aggregate_weather_daily_none_values() -> None:
    data = {
        "hourly": {
            "time": ["2024-01-15T06:00", "2024-01-15T12:00"],
            "temperature_2m": [20.0, None],
            "precipitation": [None, None],
            "relative_humidity_2m": [80.0, 70.0],
            "wind_speed_10m": [],
            "cloud_cover": [50.0, 50.0],
            "pressure_msl": [1010.0, 1012.0],
        }
    }
    forecasts = _aggregate_weather_daily(data)
    assert len(forecasts) == 1
    assert forecasts[0].temperature == pytest.approx(20.0)
    assert forecasts[0].precipitation is None
    assert forecasts[0].wind_speed is None
    assert forecasts[0].humidity == pytest.approx(75.0)


# ---------------------------------------------------------------------------
# Task tests
# ---------------------------------------------------------------------------


def test_ensure_dhis2_metadata() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_OU_WITH_GEOM
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE

    org_units = ensure_dhis2_metadata.fn(mock_client, 4)

    assert len(org_units) == 1
    assert org_units[0].id == "ROOT_OU"

    payload = mock_client.post_metadata.call_args[0][0]
    assert len(payload["dataElements"]) == 6
    assert len(payload["dataSets"]) == 1
    ds = payload["dataSets"][0]
    assert ds["name"] == "PR: OM: WF: Weather Forecast"
    assert ds["periodType"] == "Daily"
    assert ds["openFuturePeriods"] == 17
    assert len(ds["dataSetElements"]) == 6


def test_ensure_dhis2_metadata_no_polygon() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_OU_POINT

    with pytest.raises(ValueError, match="No level-4 org units with Polygon"):
        ensure_dhis2_metadata.fn(mock_client, 4)


def test_build_data_values() -> None:
    ou = OrgUnitGeo(id="ROOT_OU", name="Freetown", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])
    forecasts = [
        DailyWeather(
            date="2024-01-15",
            temperature=28.0,
            precipitation=0.5,
            humidity=70.0,
            wind_speed=4.0,
            cloud_cover=30.0,
            pressure=1011.0,
        ),
        DailyWeather(
            date="2024-01-16",
            temperature=25.0,
            precipitation=5.0,
            humidity=85.0,
            wind_speed=2.0,
            cloud_cover=90.0,
            pressure=1008.0,
        ),
    ]
    dvs = build_data_values.fn([(ou, forecasts)])

    assert isinstance(dvs, Dhis2DataValueSet)
    assert len(dvs.dataValues) == 12  # 6 vars * 2 days

    periods = {dv.period for dv in dvs.dataValues}
    assert periods == {"20240115", "20240116"}

    de_uids = {dv.dataElement for dv in dvs.dataValues}
    assert de_uids == {
        TEMPERATURE_DE_UID,
        PRECIPITATION_DE_UID,
        HUMIDITY_DE_UID,
        WIND_SPEED_DE_UID,
        CLOUD_COVER_DE_UID,
        PRESSURE_DE_UID,
    }


def test_build_data_values_skips_none() -> None:
    ou = OrgUnitGeo(id="ROOT_OU", name="Freetown", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])
    forecasts = [
        DailyWeather(
            date="2024-01-15",
            temperature=28.0,
            precipitation=None,
            humidity=None,
            wind_speed=None,
            cloud_cover=None,
            pressure=None,
        ),
    ]
    dvs = build_data_values.fn([(ou, forecasts)])
    assert len(dvs.dataValues) == 1
    assert dvs.dataValues[0].dataElement == TEMPERATURE_DE_UID


def test_import_to_dhis2() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE

    org_units = [OrgUnitGeo(id="ROOT_OU", name="Freetown", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])]
    dvs = Dhis2DataValueSet(
        dataValues=[
            _make_data_value(TEMPERATURE_DE_UID, "20240115", "28.0"),
            _make_data_value(PRECIPITATION_DE_UID, "20240115", "0.5"),
        ]
    )

    result = import_to_dhis2.fn(mock_client, "https://dhis2.example.org", org_units, dvs)

    assert isinstance(result, ImportResult)
    assert result.imported == 12
    assert result.total == 2
    assert "Open-Meteo" in result.markdown
    assert "Weather Forecast" in result.markdown
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


@patch("dhis2_openmeteo_forecast_import.fetch_daily_forecast")
@patch("dhis2_openmeteo_forecast_import.centroid")
@patch.object(Dhis2Credentials, "get_client")
def test_flow_runs(
    mock_get_client: MagicMock,
    mock_centroid: MagicMock,
    mock_fetch: MagicMock,
) -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_OU_WITH_GEOM
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE
    mock_get_client.return_value = mock_client

    mock_centroid.return_value = (8.45, -13.25)
    mock_fetch.return_value = [
        DailyWeather(
            date="2024-01-15",
            temperature=28.0,
            precipitation=0.5,
            humidity=70.0,
            wind_speed=4.0,
            cloud_cover=30.0,
            pressure=1011.0,
        ),
        DailyWeather(
            date="2024-01-16",
            temperature=25.0,
            precipitation=5.0,
            humidity=85.0,
            wind_speed=2.0,
            cloud_cover=90.0,
            pressure=1008.0,
        ),
    ]

    state = dhis2_openmeteo_forecast_import_flow(
        query=ClimateQuery(org_unit_level=4),
        return_state=True,
    )
    assert state.is_completed()
    result = state.result()
    assert isinstance(result, ImportResult)
    assert result.imported == 12
    assert len(result.org_units) == 1

    mock_centroid.assert_called_once()
    mock_fetch.assert_called_once_with(8.45, -13.25)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_data_value(de_uid: str, period: str, value: str = "25.0") -> DataValue:
    return DataValue(
        dataElement=de_uid,
        period=period,
        orgUnit="ROOT_OU",
        categoryOptionCombo="",
        value=value,
    )
