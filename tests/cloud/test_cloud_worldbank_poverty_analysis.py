"""Tests for World Bank Poverty Headcount Analysis flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "cloud_worldbank_poverty_analysis",
    Path(__file__).resolve().parent.parent.parent / "flows" / "cloud" / "cloud_worldbank_poverty_analysis.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["cloud_worldbank_poverty_analysis"] = _mod
_spec.loader.exec_module(_mod)

PovertyQuery = _mod.PovertyQuery
CountryPoverty = _mod.CountryPoverty
PovertyReport = _mod.PovertyReport
fetch_poverty_data = _mod.fetch_poverty_data
aggregate_poverty = _mod.aggregate_poverty
build_poverty_report = _mod.build_poverty_report
worldbank_poverty_analysis_flow = _mod.worldbank_poverty_analysis_flow


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_poverty_query_defaults() -> None:
    q = PovertyQuery()
    assert q.iso3_codes == ["ETH", "KEN", "TZA", "NGA", "IND", "BGD", "VNM"]
    assert q.start_year == 2010
    assert q.end_year == 2023


# ---------------------------------------------------------------------------
# Task tests
# ---------------------------------------------------------------------------


@patch("cloud_worldbank_poverty_analysis.httpx.Client")
def test_fetch_poverty_data(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {"page": 1, "pages": 1, "per_page": 50, "total": 3},
        [
            {
                "country": {"value": "Ethiopia"},
                "date": "2020",
                "value": 30.0,
            },
            {
                "country": {"value": "Ethiopia"},
                "date": "2018",
                "value": None,
            },
            {
                "country": {"value": "Ethiopia"},
                "date": "2015",
                "value": 35.0,
            },
            {
                "country": {"value": "Ethiopia"},
                "date": "2012",
                "value": 40.0,
            },
        ],
    ]
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = fetch_poverty_data.fn("ETH", 2010, 2023)

    assert result.iso3 == "ETH"
    assert result.country_name == "Ethiopia"
    assert result.latest_value == 30.0
    assert result.latest_year == 2020
    assert result.data_points == 3  # one null filtered out
    assert result.trend == "improving"  # 40.0 -> 30.0, diff=-10 < -1


@patch("cloud_worldbank_poverty_analysis.httpx.Client")
def test_fetch_poverty_data_no_data(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {"page": 1, "pages": 1, "per_page": 50, "total": 2},
        [
            {
                "country": {"value": "Unknown"},
                "date": "2020",
                "value": None,
            },
            {
                "country": {"value": "Unknown"},
                "date": "2019",
                "value": None,
            },
        ],
    ]
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = fetch_poverty_data.fn("XXX", 2010, 2023)

    assert result.latest_value is None
    assert result.latest_year is None
    assert result.data_points == 0
    assert result.trend == "unknown"


@patch("cloud_worldbank_poverty_analysis.httpx.Client")
def test_fetch_poverty_data_improving_trend(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {"page": 1, "pages": 1, "per_page": 50, "total": 2},
        [
            {
                "country": {"value": "TestCountry"},
                "date": "2020",
                "value": 20.0,
            },
            {
                "country": {"value": "TestCountry"},
                "date": "2015",
                "value": 30.0,
            },
        ],
    ]
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = fetch_poverty_data.fn("TST", 2010, 2023)

    assert result.trend == "improving"  # 30.0 -> 20.0, diff=-10 < -1


@patch("cloud_worldbank_poverty_analysis.httpx.Client")
def test_fetch_poverty_data_worsening_trend(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {"page": 1, "pages": 1, "per_page": 50, "total": 2},
        [
            {
                "country": {"value": "TestCountry"},
                "date": "2020",
                "value": 25.0,
            },
            {
                "country": {"value": "TestCountry"},
                "date": "2015",
                "value": 10.0,
            },
        ],
    ]
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = fetch_poverty_data.fn("TST", 2010, 2023)

    assert result.trend == "worsening"  # 10.0 -> 25.0, diff=15 > 1


def test_aggregate_poverty() -> None:
    countries = [
        CountryPoverty(iso3="AAA", latest_value=40.0, data_points=3),
        CountryPoverty(iso3="BBB", latest_value=None, data_points=0),
        CountryPoverty(iso3="CCC", latest_value=10.0, data_points=2),
        CountryPoverty(iso3="DDD", latest_value=25.0, data_points=1),
    ]
    result = aggregate_poverty.fn(countries)

    # Countries with data come first, sorted by rate ascending (best first)
    assert result[0].iso3 == "CCC"  # 10.0%
    assert result[1].iso3 == "DDD"  # 25.0%
    assert result[2].iso3 == "AAA"  # 40.0%
    # Countries without data come last
    assert result[3].iso3 == "BBB"


def test_build_poverty_report() -> None:
    countries = [
        CountryPoverty(
            iso3="VNM",
            country_name="Vietnam",
            latest_value=1.2,
            latest_year=2020,
            data_points=3,
            trend="improving",
        ),
        CountryPoverty(
            iso3="ETH",
            country_name="Ethiopia",
            latest_value=30.5,
            latest_year=2018,
            data_points=2,
            trend="stable",
        ),
        CountryPoverty(
            iso3="XXX",
            country_name="Unknown",
            latest_value=None,
            latest_year=None,
            data_points=0,
            trend="unknown",
        ),
    ]
    markdown = build_poverty_report.fn(countries)

    assert "Vietnam" in markdown
    assert "Ethiopia" in markdown
    assert "1.2%" in markdown
    assert "30.5%" in markdown
    assert "N/A" in markdown
    assert "Countries with data" in markdown
    assert "2/3" in markdown
    assert "Average poverty rate" in markdown
    assert "Best performer" in markdown
    assert "Worst performer" in markdown


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("cloud_worldbank_poverty_analysis.httpx.Client")
def test_flow_runs(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {"page": 1, "pages": 1, "per_page": 50, "total": 2},
        [
            {
                "country": {"value": "TestA"},
                "date": "2020",
                "value": 15.0,
            },
            {
                "country": {"value": "TestA"},
                "date": "2015",
                "value": 25.0,
            },
        ],
    ]
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    query = PovertyQuery(iso3_codes=["AAA", "BBB"], start_year=2010, end_year=2023)
    state = worldbank_poverty_analysis_flow(query=query, return_state=True)
    assert state.is_completed()
