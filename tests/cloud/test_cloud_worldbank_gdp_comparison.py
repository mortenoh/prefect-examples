"""Tests for World Bank GDP Comparison flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "cloud_worldbank_gdp_comparison",
    Path(__file__).resolve().parent.parent.parent / "flows" / "cloud" / "cloud_worldbank_gdp_comparison.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["cloud_worldbank_gdp_comparison"] = _mod
_spec.loader.exec_module(_mod)

GdpComparisonQuery = _mod.GdpComparisonQuery
CountryGdp = _mod.CountryGdp
GdpComparisonReport = _mod.GdpComparisonReport
fetch_gdp_data = _mod.fetch_gdp_data
rank_by_gdp = _mod.rank_by_gdp
build_gdp_report = _mod.build_gdp_report
worldbank_gdp_comparison_flow = _mod.worldbank_gdp_comparison_flow


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_gdp_comparison_query_defaults() -> None:
    q = GdpComparisonQuery(iso3_codes=["USA", "CHN"])
    assert q.year == 2023


def test_country_gdp_model() -> None:
    c = CountryGdp(iso3="USA", country_name="United States", gdp_usd=25000000000000.0, year=2023)
    assert c.iso3 == "USA"
    assert c.country_name == "United States"
    assert c.gdp_usd == 25000000000000.0
    assert c.year == 2023


# ---------------------------------------------------------------------------
# Task tests
# ---------------------------------------------------------------------------


@patch("cloud_worldbank_gdp_comparison.httpx.Client")
def test_fetch_gdp_data(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {"page": 1, "pages": 1, "per_page": 2, "total": 2},
        [
            {
                "countryiso3code": "USA",
                "country": {"value": "United States"},
                "value": 25000000000000.0,
                "date": "2023",
            },
            {
                "countryiso3code": "CHN",
                "country": {"value": "China"},
                "value": 18000000000000.0,
                "date": "2023",
            },
        ],
    ]
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = fetch_gdp_data.fn(["USA", "CHN"], 2023)

    assert len(result) == 2
    assert result[0].iso3 == "USA"
    assert result[0].gdp_usd == 25000000000000.0
    assert result[1].iso3 == "CHN"
    assert result[1].country_name == "China"


@patch("cloud_worldbank_gdp_comparison.httpx.Client")
def test_fetch_gdp_data_empty(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {"page": 1, "pages": 0, "per_page": 2, "total": 0},
        None,
    ]
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = fetch_gdp_data.fn(["XXX"], 2023)

    assert result == []


def test_rank_by_gdp() -> None:
    countries = [
        CountryGdp(iso3="DEU", country_name="Germany", gdp_usd=4000000000000.0, year=2023),
        CountryGdp(iso3="USA", country_name="United States", gdp_usd=25000000000000.0, year=2023),
        CountryGdp(iso3="JPN", country_name="Japan", gdp_usd=4200000000000.0, year=2023),
    ]
    ranked = rank_by_gdp.fn(countries)

    assert ranked[0].iso3 == "USA"
    assert ranked[1].iso3 == "JPN"
    assert ranked[2].iso3 == "DEU"


def test_build_gdp_report() -> None:
    countries = [
        CountryGdp(iso3="USA", country_name="United States", gdp_usd=25000000000000.0, year=2023),
        CountryGdp(iso3="CHN", country_name="China", gdp_usd=18000000000000.0, year=2023),
    ]
    query = GdpComparisonQuery(iso3_codes=["USA", "CHN"])

    markdown = build_gdp_report.fn(countries, query)

    assert "USA" in markdown
    assert "CHN" in markdown
    assert "United States" in markdown
    assert "China" in markdown
    assert "Rank" in markdown
    assert "GDP (US$)" in markdown
    assert "Largest economy" in markdown
    assert "Smallest economy" in markdown
    assert "Total GDP" in markdown


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("cloud_worldbank_gdp_comparison.httpx.Client")
def test_flow_runs(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {"page": 1, "pages": 1, "per_page": 2, "total": 2},
        [
            {
                "countryiso3code": "USA",
                "country": {"value": "United States"},
                "value": 25000000000000.0,
                "date": "2023",
            },
            {
                "countryiso3code": "CHN",
                "country": {"value": "China"},
                "value": 18000000000000.0,
                "date": "2023",
            },
        ],
    ]
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    query = GdpComparisonQuery(iso3_codes=["USA", "CHN"])
    state = worldbank_gdp_comparison_flow(query=query, return_state=True)
    assert state.is_completed()
