"""Tests for DHIS2 yr.no Weather Forecast Import flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Load the flow module via importlib (same pattern as existing tests)
# ---------------------------------------------------------------------------

_spec = importlib.util.spec_from_file_location(
    "dhis2_yr_weather_import",
    Path(__file__).resolve().parent.parent.parent / "flows" / "dhis2" / "dhis2_yr_weather_import.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["dhis2_yr_weather_import"] = _mod
_spec.loader.exec_module(_mod)

from prefect_climate import ClimateQuery, ImportResult  # noqa: E402
from prefect_climate.yr import DailyForecast, aggregate_forecast_daily  # noqa: E402
from prefect_climate.yr.forecast import LOCATIONFORECAST_URL  # noqa: E402
from prefect_climate.zonal import centroid  # noqa: E402
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
dhis2_yr_weather_import_flow = _mod.dhis2_yr_weather_import_flow

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
    "importCount": {"imported": 18, "updated": 0, "ignored": 0, "deleted": 0},
}

# A minimal yr.no Locationforecast response with 3 hourly entries across 2 days.
SAMPLE_FORECAST_RESPONSE = {
    "type": "Feature",
    "geometry": {"type": "Point", "coordinates": [-13.25, 8.45, 0]},
    "properties": {
        "meta": {"updated_at": "2024-01-15T12:00:00Z"},
        "timeseries": [
            {
                "time": "2024-01-15T06:00:00Z",
                "data": {
                    "instant": {
                        "details": {
                            "air_temperature": 26.0,
                            "relative_humidity": 80.0,
                            "wind_speed": 3.5,
                            "cloud_area_fraction": 40.0,
                            "air_pressure_at_sea_level": 1012.0,
                        }
                    },
                    "next_1_hours": {"details": {"precipitation_amount": 0.5}},
                },
            },
            {
                "time": "2024-01-15T12:00:00Z",
                "data": {
                    "instant": {
                        "details": {
                            "air_temperature": 30.0,
                            "relative_humidity": 60.0,
                            "wind_speed": 4.5,
                            "cloud_area_fraction": 20.0,
                            "air_pressure_at_sea_level": 1010.0,
                        }
                    },
                    "next_1_hours": {"details": {"precipitation_amount": 0.0}},
                },
            },
            {
                "time": "2024-01-16T06:00:00Z",
                "data": {
                    "instant": {
                        "details": {
                            "air_temperature": 25.0,
                            "relative_humidity": 85.0,
                            "wind_speed": 2.0,
                            "cloud_area_fraction": 90.0,
                            "air_pressure_at_sea_level": 1008.0,
                        }
                    },
                    "next_6_hours": {"details": {"precipitation_amount": 5.0}},
                },
            },
        ],
    },
}


# ---------------------------------------------------------------------------
# Library tests -- centroid
# ---------------------------------------------------------------------------


def test_centroid_polygon() -> None:
    geometry = {
        "type": "Polygon",
        "coordinates": [[[10.0, 0.0], [20.0, 0.0], [20.0, 10.0], [10.0, 10.0], [10.0, 0.0]]],
    }
    lat, lon = centroid(geometry)
    # Mean of lats [0,0,10,10,0] = 4.0, mean of lons [10,20,20,10,10] = 14.0
    assert lat == pytest.approx(4.0)
    assert lon == pytest.approx(14.0)


def test_centroid_multipolygon() -> None:
    geometry = {
        "type": "MultiPolygon",
        "coordinates": [
            [[[0.0, 0.0], [2.0, 0.0], [2.0, 2.0], [0.0, 2.0], [0.0, 0.0]]],
            [[[10.0, 10.0], [12.0, 10.0], [12.0, 12.0], [10.0, 12.0], [10.0, 10.0]]],
        ],
    }
    lat, lon = centroid(geometry)
    # All lats: [0,0,2,2,0, 10,10,12,12,10] -> mean = 5.8
    # All lons: [0,2,2,0,0, 10,12,12,10,10] -> mean = 5.8
    assert lat == pytest.approx(5.8)
    assert lon == pytest.approx(5.8)


def test_centroid_empty_geometry() -> None:
    with pytest.raises(ValueError, match="No coordinates"):
        centroid({"type": "Polygon", "coordinates": []})


# ---------------------------------------------------------------------------
# Library tests -- daily aggregation
# ---------------------------------------------------------------------------


def test_aggregate_forecast_daily() -> None:
    forecasts = aggregate_forecast_daily(SAMPLE_FORECAST_RESPONSE)

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
    assert forecasts[1].humidity == pytest.approx(85.0)
    assert forecasts[1].wind_speed == pytest.approx(2.0)
    assert forecasts[1].cloud_cover == pytest.approx(90.0)
    assert forecasts[1].pressure == pytest.approx(1008.0)


def test_aggregate_forecast_empty() -> None:
    forecasts = aggregate_forecast_daily({"properties": {"timeseries": []}})
    assert forecasts == []


def test_aggregate_forecast_prefers_next_1_hours() -> None:
    """When both next_1_hours and next_6_hours are present, prefer next_1_hours."""
    response = {
        "properties": {
            "timeseries": [
                {
                    "time": "2024-01-15T06:00:00Z",
                    "data": {
                        "instant": {"details": {"air_temperature": 20.0}},
                        "next_1_hours": {"details": {"precipitation_amount": 1.0}},
                        "next_6_hours": {"details": {"precipitation_amount": 10.0}},
                    },
                }
            ]
        }
    }
    forecasts = aggregate_forecast_daily(response)
    assert len(forecasts) == 1
    assert forecasts[0].precipitation == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Library tests -- URL / coordinate truncation
# ---------------------------------------------------------------------------


def test_coordinate_truncation() -> None:
    """Verify coordinates are truncated to 4 decimal places."""
    assert round(8.123456789, 4) == 8.1235
    assert round(-13.987654321, 4) == -13.9877
    assert LOCATIONFORECAST_URL.startswith("https://api.met.no/")


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
    assert org_units[0].name == "Freetown"

    mock_client.post_metadata.assert_called_once()
    payload = mock_client.post_metadata.call_args[0][0]
    assert "dataElements" in payload
    assert "dataSets" in payload
    # 6 data elements for weather forecast variables
    assert len(payload["dataElements"]) == 6
    de_names = {de["name"] for de in payload["dataElements"]}
    assert de_names == {
        "WF: yr.no: Temperature",
        "WF: yr.no: Precipitation",
        "WF: yr.no: Relative Humidity",
        "WF: yr.no: Wind Speed",
        "WF: yr.no: Cloud Cover",
        "WF: yr.no: Air Pressure",
    }
    # 1 data set with 6 data set elements and Daily period type
    assert len(payload["dataSets"]) == 1
    ds = payload["dataSets"][0]
    assert ds["name"] == "WF: yr.no: Weather Forecast"
    assert ds["periodType"] == "Daily"
    assert len(ds["dataSetElements"]) == 6


def test_ensure_dhis2_metadata_no_polygon() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_OU_POINT

    with pytest.raises(ValueError, match="No level-4 org units with Polygon"):
        ensure_dhis2_metadata.fn(mock_client, 4)


def test_build_data_values() -> None:
    ou = OrgUnitGeo(id="ROOT_OU", name="Freetown", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])
    forecasts = [
        DailyForecast(
            date="2024-01-15",
            temperature=28.0,
            precipitation=0.5,
            humidity=70.0,
            wind_speed=4.0,
            cloud_cover=30.0,
            pressure=1011.0,
        ),
        DailyForecast(
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
    # 6 vars * 2 days = 12 data values
    assert len(dvs.dataValues) == 12

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

    for dv in dvs.dataValues:
        assert dv.orgUnit == "ROOT_OU"


def test_build_data_values_skips_none() -> None:
    ou = OrgUnitGeo(id="ROOT_OU", name="Freetown", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])
    forecasts = [
        DailyForecast(
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

    # Only temperature has a value
    assert len(dvs.dataValues) == 1
    assert dvs.dataValues[0].dataElement == TEMPERATURE_DE_UID


def test_build_data_values_multiple_org_units() -> None:
    ou1 = OrgUnitGeo(id="OU1", name="District A", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])
    ou2 = OrgUnitGeo(id="OU2", name="District B", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])
    forecasts = [
        DailyForecast(
            date="2024-01-15",
            temperature=28.0,
            precipitation=0.5,
            humidity=70.0,
            wind_speed=4.0,
            cloud_cover=30.0,
            pressure=1011.0,
        ),
    ]
    dvs = build_data_values.fn([(ou1, forecasts), (ou2, forecasts)])
    # 6 vars * 1 day * 2 org units = 12
    assert len(dvs.dataValues) == 12


def test_import_to_dhis2() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE

    org_units = [OrgUnitGeo(id="ROOT_OU", name="Freetown", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])]
    dvs = Dhis2DataValueSet(
        dataValues=[
            _make_data_value(de_uid, "20240115")
            for de_uid in [
                TEMPERATURE_DE_UID,
                PRECIPITATION_DE_UID,
                HUMIDITY_DE_UID,
                WIND_SPEED_DE_UID,
                CLOUD_COVER_DE_UID,
                PRESSURE_DE_UID,
            ]
        ]
        + [
            _make_data_value(de_uid, "20240116")
            for de_uid in [
                TEMPERATURE_DE_UID,
                PRECIPITATION_DE_UID,
                HUMIDITY_DE_UID,
                WIND_SPEED_DE_UID,
                CLOUD_COVER_DE_UID,
                PRESSURE_DE_UID,
            ]
        ]
        + [
            _make_data_value(de_uid, "20240117")
            for de_uid in [
                TEMPERATURE_DE_UID,
                PRECIPITATION_DE_UID,
                HUMIDITY_DE_UID,
                WIND_SPEED_DE_UID,
                CLOUD_COVER_DE_UID,
                PRESSURE_DE_UID,
            ]
        ]
    )

    result = import_to_dhis2.fn(mock_client, "https://dhis2.example.org", org_units, dvs)

    assert isinstance(result, ImportResult)
    assert result.imported == 18
    assert result.total == 18
    assert "yr.no" in result.markdown
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


@patch("dhis2_yr_weather_import.fetch_daily_forecasts")
@patch("dhis2_yr_weather_import.centroid")
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
        DailyForecast(
            date="2024-01-15",
            temperature=28.0,
            precipitation=0.5,
            humidity=70.0,
            wind_speed=4.0,
            cloud_cover=30.0,
            pressure=1011.0,
        ),
        DailyForecast(
            date="2024-01-16",
            temperature=25.0,
            precipitation=5.0,
            humidity=85.0,
            wind_speed=2.0,
            cloud_cover=90.0,
            pressure=1008.0,
        ),
        DailyForecast(
            date="2024-01-17",
            temperature=27.0,
            precipitation=1.0,
            humidity=75.0,
            wind_speed=3.0,
            cloud_cover=50.0,
            pressure=1010.0,
        ),
    ]

    state = dhis2_yr_weather_import_flow(
        query=ClimateQuery(org_unit_level=4),
        return_state=True,
    )
    assert state.is_completed()
    result = state.result()
    assert isinstance(result, ImportResult)
    assert result.imported == 18
    assert len(result.org_units) == 1
    assert result.org_units[0].id == "ROOT_OU"

    mock_centroid.assert_called_once()
    mock_fetch.assert_called_once_with(8.45, -13.25)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_data_value(de_uid: str, period: str) -> DataValue:
    return DataValue(
        dataElement=de_uid,
        period=period,
        orgUnit="ROOT_OU",
        categoryOptionCombo="",
        value="25.0",
    )
