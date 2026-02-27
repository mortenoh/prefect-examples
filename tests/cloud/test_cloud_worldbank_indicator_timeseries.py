"""Tests for World Bank Indicator Time-Series flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "cloud_worldbank_indicator_timeseries",
    Path(__file__).resolve().parent.parent.parent / "flows" / "cloud" / "cloud_worldbank_indicator_timeseries.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["cloud_worldbank_indicator_timeseries"] = _mod
_spec.loader.exec_module(_mod)

TimeseriesQuery = _mod.TimeseriesQuery
YearValue = _mod.YearValue
GrowthRate = _mod.GrowthRate
TimeseriesReport = _mod.TimeseriesReport
fetch_indicator_timeseries = _mod.fetch_indicator_timeseries
compute_growth_rates = _mod.compute_growth_rates
build_timeseries_report = _mod.build_timeseries_report
worldbank_indicator_timeseries_flow = _mod.worldbank_indicator_timeseries_flow


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_timeseries_query_defaults() -> None:
    q = TimeseriesQuery()
    assert q.iso3 == "ETH"
    assert q.indicator == "SP.POP.TOTL"
    assert q.start_year == 2000
    assert q.end_year == 2023


# ---------------------------------------------------------------------------
# Task tests
# ---------------------------------------------------------------------------


@patch("cloud_worldbank_indicator_timeseries.httpx.Client")
def test_fetch_indicator_timeseries(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {"page": 1, "pages": 1, "per_page": 100, "total": 3},
        [
            {"date": "2002", "value": 300},
            {"date": "2000", "value": 100},
            {"date": "2001", "value": 200},
        ],
    ]
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = fetch_indicator_timeseries.fn("ETH", "SP.POP.TOTL", 2000, 2002)
    assert len(result) == 3
    # Verify sorted by year ascending
    assert result[0].year == 2000
    assert result[1].year == 2001
    assert result[2].year == 2002
    assert result[0].value == 100
    assert result[2].value == 300


@patch("cloud_worldbank_indicator_timeseries.httpx.Client")
def test_fetch_indicator_timeseries_null_values(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {"page": 1, "pages": 1, "per_page": 100, "total": 4},
        [
            {"date": "2000", "value": 100},
            {"date": "2001", "value": None},
            {"date": "2002", "value": 200},
            {"date": "2003", "value": None},
        ],
    ]
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = fetch_indicator_timeseries.fn("ETH", "SP.POP.TOTL", 2000, 2003)
    assert len(result) == 2
    assert result[0].year == 2000
    assert result[1].year == 2002


def test_compute_growth_rates() -> None:
    data = [
        YearValue(year=2000, value=100),
        YearValue(year=2001, value=110),
        YearValue(year=2002, value=121),
    ]
    rates = compute_growth_rates.fn(data)

    assert len(rates) == 2
    assert rates[0].year == 2001
    assert rates[0].value == 110
    assert rates[0].growth_pct == 10.0
    assert rates[1].year == 2002
    assert rates[1].value == 121
    assert rates[1].growth_pct == 10.0


def test_build_timeseries_report() -> None:
    data = [
        GrowthRate(year=2001, value=110, growth_pct=10.0),
        GrowthRate(year=2002, value=105, growth_pct=-4.5455),
        GrowthRate(year=2003, value=120, growth_pct=14.2857),
    ]
    report = build_timeseries_report.fn("ETH", "SP.POP.TOTL", data)

    assert isinstance(report, TimeseriesReport)
    assert report.iso3 == "ETH"
    assert report.indicator == "SP.POP.TOTL"
    assert len(report.data_points) == 3
    # Peak is 2003, trough is 2002
    assert report.peak_growth is not None
    assert report.peak_growth.year == 2003
    assert report.trough_growth is not None
    assert report.trough_growth.year == 2002
    # Markdown content checks
    assert "ETH" in report.markdown
    assert "SP.POP.TOTL" in report.markdown
    assert "2001" in report.markdown
    assert "Peak year" in report.markdown
    assert "Trough year" in report.markdown
    assert "Average growth" in report.markdown
    assert "Total data points" in report.markdown


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("cloud_worldbank_indicator_timeseries.httpx.Client")
def test_flow_runs(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {"page": 1, "pages": 1, "per_page": 100, "total": 3},
        [
            {"date": "2000", "value": 100},
            {"date": "2001", "value": 110},
            {"date": "2002", "value": 121},
        ],
    ]
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    state = worldbank_indicator_timeseries_flow(return_state=True)
    assert state.is_completed()
