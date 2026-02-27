"""Tests for WorldPop Population Time-Series flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "cloud_worldpop_population_timeseries",
    Path(__file__).resolve().parent.parent.parent / "flows" / "cloud" / "cloud_worldpop_population_timeseries.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["cloud_worldpop_population_timeseries"] = _mod
_spec.loader.exec_module(_mod)

TimeseriesQuery = _mod.TimeseriesQuery
YearMetadata = _mod.YearMetadata
GrowthRate = _mod.GrowthRate
TimeseriesReport = _mod.TimeseriesReport
fetch_country_years = _mod.fetch_country_years
extract_year_metadata = _mod.extract_year_metadata
compute_growth_rates = _mod.compute_growth_rates
build_timeseries_report = _mod.build_timeseries_report
worldpop_population_timeseries_flow = _mod.worldpop_population_timeseries_flow


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_timeseries_query_defaults() -> None:
    q = TimeseriesQuery(iso3="ETH")
    assert q.dataset == "pop"
    assert q.subdataset == "wpgp"


def test_year_metadata_model() -> None:
    m = YearMetadata(year=2020, title="Ethiopia 2020", doi="10.xxxx")
    assert m.year == 2020


def test_growth_rate_model() -> None:
    g = GrowthRate(from_year=2019, to_year=2020, growth_rate_pct=2.5)
    assert g.growth_rate_pct == 2.5


# ---------------------------------------------------------------------------
# Task tests
# ---------------------------------------------------------------------------


@patch("cloud_worldpop_population_timeseries.httpx.Client")
def test_fetch_country_years(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "data": [
            {"title": "ETH 2018", "popyear": 2018, "doi": ""},
            {"title": "ETH 2019", "popyear": 2019, "doi": ""},
            {"title": "ETH 2020", "popyear": 2020, "doi": ""},
        ]
    }
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = fetch_country_years.fn("ETH", "pop", "wpgp")
    assert len(result) == 3


def test_extract_year_metadata() -> None:
    items = [
        {"title": "ETH 2020", "popyear": 2020, "doi": "10.xxx"},
        {"title": "ETH 2018", "popyear": 2018, "doi": "10.yyy"},
        {"title": "ETH 2019", "popyear": 2019, "doi": "10.zzz"},
    ]
    records = extract_year_metadata.fn(items)

    assert len(records) == 3
    assert records[0].year == 2018  # sorted
    assert records[1].year == 2019
    assert records[2].year == 2020


def test_extract_year_metadata_skips_missing_year() -> None:
    items = [
        {"title": "No Year", "doi": ""},
        {"title": "ETH 2020", "popyear": 2020, "doi": ""},
    ]
    records = extract_year_metadata.fn(items)
    assert len(records) == 1
    assert records[0].year == 2020


def test_compute_growth_rates() -> None:
    year_records = [
        YearMetadata(year=2018, title="", doi=""),
        YearMetadata(year=2019, title="", doi=""),
        YearMetadata(year=2020, title="", doi=""),
    ]
    rates = compute_growth_rates.fn(year_records)

    assert len(rates) == 2
    assert rates[0].from_year == 2018
    assert rates[0].to_year == 2019
    assert rates[0].growth_rate_pct == 2.5  # 1 year gap
    assert rates[1].from_year == 2019
    assert rates[1].to_year == 2020


def test_compute_growth_rates_with_gap() -> None:
    year_records = [
        YearMetadata(year=2010, title="", doi=""),
        YearMetadata(year=2015, title="", doi=""),
    ]
    rates = compute_growth_rates.fn(year_records)

    assert len(rates) == 1
    # 5-year gap should compound
    assert rates[0].growth_rate_pct > 2.5


def test_compute_growth_rates_single_year() -> None:
    year_records = [YearMetadata(year=2020, title="", doi="")]
    rates = compute_growth_rates.fn(year_records)
    assert len(rates) == 0


def test_build_timeseries_report() -> None:
    year_records = [
        YearMetadata(year=2018, title="ETH 2018", doi=""),
        YearMetadata(year=2019, title="ETH 2019", doi=""),
        YearMetadata(year=2020, title="ETH 2020", doi=""),
    ]
    growth_rates = [
        GrowthRate(from_year=2018, to_year=2019, growth_rate_pct=2.5),
        GrowthRate(from_year=2019, to_year=2020, growth_rate_pct=2.5),
    ]
    report = build_timeseries_report.fn("ETH", year_records, growth_rates)

    assert isinstance(report, TimeseriesReport)
    assert report.iso3 == "ETH"
    assert report.years_available == 3
    assert report.peak_growth_year == 2019  # first max
    assert report.peak_growth_rate == 2.5
    assert "ETH" in report.markdown
    assert "2018" in report.markdown
    assert "2.50%" in report.markdown


def test_build_timeseries_report_no_growth() -> None:
    year_records = [YearMetadata(year=2020, title="ETH 2020", doi="")]
    report = build_timeseries_report.fn("ETH", year_records, [])

    assert report.peak_growth_year is None
    assert report.peak_growth_rate is None


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("cloud_worldpop_population_timeseries.httpx.Client")
def test_flow_runs(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "data": [
            {"title": "ETH 2019", "popyear": 2019, "doi": ""},
            {"title": "ETH 2020", "popyear": 2020, "doi": ""},
        ]
    }
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    state = worldpop_population_timeseries_flow(return_state=True)
    assert state.is_completed()
