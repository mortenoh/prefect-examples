"""Tests for DHIS2 Open-Meteo Air Quality Import flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Load the flow module via importlib
# ---------------------------------------------------------------------------

_spec = importlib.util.spec_from_file_location(
    "dhis2_openmeteo_air_quality_import",
    Path(__file__).resolve().parent.parent.parent
    / "deployments"
    / "dhis2_openmeteo_import_docker"
    / "dhis2_openmeteo_air_quality_import.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["dhis2_openmeteo_air_quality_import"] = _mod
_spec.loader.exec_module(_mod)

from prefect_climate import ClimateQuery, ImportResult  # noqa: E402
from prefect_climate.openmeteo import DailyAirQuality  # noqa: E402
from prefect_dhis2 import (  # noqa: E402
    DataValue,
    Dhis2Client,
    Dhis2DataValueSet,
    OrgUnitGeo,
)
from prefect_dhis2.credentials import Dhis2Credentials  # noqa: E402

ensure_dhis2_metadata = _mod.ensure_dhis2_metadata
fetch_org_unit_air_quality = _mod.fetch_org_unit_air_quality
build_data_values = _mod.build_data_values
import_to_dhis2 = _mod.import_to_dhis2
dhis2_openmeteo_air_quality_import_flow = _mod.dhis2_openmeteo_air_quality_import_flow

PM2_5_DE_UID = _mod.PM2_5_DE_UID
PM10_DE_UID = _mod.PM10_DE_UID
OZONE_DE_UID = _mod.OZONE_DE_UID
NO2_DE_UID = _mod.NO2_DE_UID
SO2_DE_UID = _mod.SO2_DE_UID
CO_DE_UID = _mod.CO_DE_UID
AQI_DE_UID = _mod.AQI_DE_UID
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

SAMPLE_OU_NO_GEOM = [
    {
        "id": "NO_GEOM_OU",
        "name": "No Geometry Org",
    }
]

SAMPLE_METADATA_RESPONSE = {
    "status": "OK",
    "stats": {"created": 8, "updated": 0, "deleted": 0, "ignored": 0, "total": 8},
}

SAMPLE_IMPORT_RESPONSE = {
    "status": "SUCCESS",
    "importCount": {"imported": 14, "updated": 0, "ignored": 0, "deleted": 0},
}

# Minimal Open-Meteo air quality response with hourly entries across 2 days.
SAMPLE_AQ_RESPONSE = {
    "hourly": {
        "time": [
            "2024-01-15T06:00",
            "2024-01-15T12:00",
            "2024-01-16T06:00",
        ],
        "pm2_5": [12.0, 18.0, 8.0],
        "pm10": [20.0, 30.0, 15.0],
        "ozone": [40.0, 60.0, 50.0],
        "nitrogen_dioxide": [10.0, 20.0, 5.0],
        "sulphur_dioxide": [3.0, 7.0, 2.0],
        "carbon_monoxide": [200.0, 300.0, 150.0],
        "european_aqi": [25.0, 45.0, 30.0],
    }
}


# ---------------------------------------------------------------------------
# Aggregation tests
# ---------------------------------------------------------------------------


def test_aggregate_air_quality_daily() -> None:
    from prefect_climate.openmeteo import air_quality as aq_mod

    # Mock the HTTP call and test aggregation
    with patch.object(aq_mod, "fetch_openmeteo_hourly", return_value=SAMPLE_AQ_RESPONSE):
        results = aq_mod.fetch_daily_air_quality(8.45, -13.25)

    assert len(results) == 2
    assert results[0].date == "2024-01-15"
    assert results[1].date == "2024-01-16"

    # Day 1: mean of [12.0, 18.0] = 15.0
    assert results[0].pm2_5 == pytest.approx(15.0)
    # Day 1: mean of [20.0, 30.0] = 25.0
    assert results[0].pm10 == pytest.approx(25.0)
    # Day 1: mean of [40.0, 60.0] = 50.0
    assert results[0].ozone == pytest.approx(50.0)
    # Day 1: mean of [10.0, 20.0] = 15.0
    assert results[0].nitrogen_dioxide == pytest.approx(15.0)
    # Day 1: mean of [3.0, 7.0] = 5.0
    assert results[0].sulphur_dioxide == pytest.approx(5.0)
    # Day 1: mean of [200.0, 300.0] = 250.0
    assert results[0].carbon_monoxide == pytest.approx(250.0)
    # Day 1: MAX of [25.0, 45.0] = 45.0 (not mean!)
    assert results[0].european_aqi == pytest.approx(45.0)

    # Day 2: single entry
    assert results[1].pm2_5 == pytest.approx(8.0)
    assert results[1].european_aqi == pytest.approx(30.0)


def test_european_aqi_uses_max() -> None:
    """Verify that european_aqi uses max aggregation, not mean."""
    from prefect_climate.openmeteo import air_quality as aq_mod

    data = {
        "hourly": {
            "time": ["2024-01-15T00:00", "2024-01-15T06:00", "2024-01-15T12:00"],
            "pm2_5": [10.0, 10.0, 10.0],
            "pm10": [],
            "ozone": [],
            "nitrogen_dioxide": [],
            "sulphur_dioxide": [],
            "carbon_monoxide": [],
            "european_aqi": [20.0, 80.0, 40.0],
        }
    }
    with patch.object(aq_mod, "fetch_openmeteo_hourly", return_value=data):
        results = aq_mod.fetch_daily_air_quality(0, 0)

    assert len(results) == 1
    # Mean of [10, 10, 10] = 10
    assert results[0].pm2_5 == pytest.approx(10.0)
    # Max of [20, 80, 40] = 80 (NOT mean of 46.7)
    assert results[0].european_aqi == pytest.approx(80.0)


def test_aggregate_air_quality_empty() -> None:
    from prefect_climate.openmeteo import air_quality as aq_mod

    with patch.object(aq_mod, "fetch_openmeteo_hourly", return_value={"hourly": {"time": []}}):
        results = aq_mod.fetch_daily_air_quality(0, 0)

    assert results == []


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
    assert len(payload["dataElements"]) == 7
    assert len(payload["dataSets"]) == 1
    ds = payload["dataSets"][0]
    assert ds["name"] == "PR: OM: AQ: Air Quality"
    assert ds["periodType"] == "Daily"
    assert ds["openFuturePeriods"] == 6
    assert len(ds["dataSetElements"]) == 7


def test_ensure_dhis2_metadata_no_geometry() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_OU_NO_GEOM

    with pytest.raises(ValueError, match="No level-4 org units with geometry"):
        ensure_dhis2_metadata.fn(mock_client, 4)


def test_build_data_values() -> None:
    ou = OrgUnitGeo(id="ROOT_OU", name="Freetown", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])
    aq_days = [
        DailyAirQuality(
            date="2024-01-15",
            pm2_5=15.0,
            pm10=25.0,
            ozone=50.0,
            nitrogen_dioxide=15.0,
            sulphur_dioxide=5.0,
            carbon_monoxide=250.0,
            european_aqi=45.0,
        ),
        DailyAirQuality(
            date="2024-01-16",
            pm2_5=8.0,
            pm10=15.0,
            ozone=50.0,
            nitrogen_dioxide=5.0,
            sulphur_dioxide=2.0,
            carbon_monoxide=150.0,
            european_aqi=30.0,
        ),
    ]
    dvs = build_data_values.fn([(ou, aq_days)])

    assert isinstance(dvs, Dhis2DataValueSet)
    assert len(dvs.dataValues) == 14  # 7 vars * 2 days

    periods = {dv.period for dv in dvs.dataValues}
    assert periods == {"20240115", "20240116"}

    de_uids = {dv.dataElement for dv in dvs.dataValues}
    assert de_uids == {
        PM2_5_DE_UID,
        PM10_DE_UID,
        OZONE_DE_UID,
        NO2_DE_UID,
        SO2_DE_UID,
        CO_DE_UID,
        AQI_DE_UID,
    }


def test_build_data_values_skips_none() -> None:
    ou = OrgUnitGeo(id="ROOT_OU", name="Freetown", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])
    aq_days = [
        DailyAirQuality(
            date="2024-01-15",
            pm2_5=15.0,
            pm10=None,
            ozone=None,
            nitrogen_dioxide=None,
            sulphur_dioxide=None,
            carbon_monoxide=None,
            european_aqi=None,
        ),
    ]
    dvs = build_data_values.fn([(ou, aq_days)])
    assert len(dvs.dataValues) == 1
    assert dvs.dataValues[0].dataElement == PM2_5_DE_UID


def test_import_to_dhis2() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE

    org_units = [OrgUnitGeo(id="ROOT_OU", name="Freetown", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])]
    dvs = Dhis2DataValueSet(
        dataValues=[
            _make_data_value(PM2_5_DE_UID, "20240115", "15.0"),
            _make_data_value(AQI_DE_UID, "20240115", "45.0"),
        ]
    )

    result = import_to_dhis2.fn(mock_client, "https://dhis2.example.org", org_units, dvs)

    assert isinstance(result, ImportResult)
    assert result.imported == 14
    assert result.total == 2
    assert "Open-Meteo" in result.markdown
    assert "Air Quality" in result.markdown
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


@patch("dhis2_openmeteo_air_quality_import.fetch_daily_air_quality")
@patch("dhis2_openmeteo_air_quality_import.centroid")
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
        DailyAirQuality(
            date="2024-01-15",
            pm2_5=15.0,
            pm10=25.0,
            ozone=50.0,
            nitrogen_dioxide=15.0,
            sulphur_dioxide=5.0,
            carbon_monoxide=250.0,
            european_aqi=45.0,
        ),
        DailyAirQuality(
            date="2024-01-16",
            pm2_5=8.0,
            pm10=15.0,
            ozone=50.0,
            nitrogen_dioxide=5.0,
            sulphur_dioxide=2.0,
            carbon_monoxide=150.0,
            european_aqi=30.0,
        ),
    ]

    state = dhis2_openmeteo_air_quality_import_flow(
        query=ClimateQuery(org_unit_level=4),
        return_state=True,
    )
    assert state.is_completed()
    result = state.result()
    assert isinstance(result, ImportResult)
    assert result.imported == 14
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
