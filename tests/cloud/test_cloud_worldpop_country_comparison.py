"""Tests for WorldPop Country Comparison flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "cloud_worldpop_country_comparison",
    Path(__file__).resolve().parent.parent.parent / "flows" / "cloud" / "cloud_worldpop_country_comparison.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["cloud_worldpop_country_comparison"] = _mod
_spec.loader.exec_module(_mod)

ComparisonQuery = _mod.ComparisonQuery
CountryMetadata = _mod.CountryMetadata
ComparisonReport = _mod.ComparisonReport
fetch_country_metadata = _mod.fetch_country_metadata
aggregate_comparison = _mod.aggregate_comparison
build_comparison_report = _mod.build_comparison_report
worldpop_country_comparison_flow = _mod.worldpop_country_comparison_flow


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_comparison_query_defaults() -> None:
    q = ComparisonQuery(iso3_codes=["ETH", "KEN"])
    assert q.dataset == "pop"
    assert q.subdataset == "wpgp"
    assert q.year == 2020


def test_country_metadata_model() -> None:
    m = CountryMetadata(iso3="ETH", title="Ethiopia 2020", popyear="2020", data_available=True)
    assert m.data_available is True


def test_country_metadata_no_data() -> None:
    m = CountryMetadata(iso3="XXX")
    assert m.data_available is False
    assert m.title == ""


# ---------------------------------------------------------------------------
# Task tests
# ---------------------------------------------------------------------------


@patch("cloud_worldpop_country_comparison.httpx.Client")
def test_fetch_country_metadata_found(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "data": [
            {"title": "Ethiopia 2020", "popyear": 2020, "doi": "10.xxxx"},
            {"title": "Ethiopia 2019", "popyear": 2019, "doi": "10.yyyy"},
        ]
    }
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = fetch_country_metadata.fn("ETH", "pop", "wpgp", 2020)

    assert result.data_available is True
    assert result.iso3 == "ETH"
    assert result.popyear == "2020"


@patch("cloud_worldpop_country_comparison.httpx.Client")
def test_fetch_country_metadata_not_found(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"data": [{"title": "Only 2019", "popyear": 2019}]}
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = fetch_country_metadata.fn("ETH", "pop", "wpgp", 2020)

    assert result.data_available is False


def test_aggregate_comparison() -> None:
    records = [
        CountryMetadata(iso3="KEN", data_available=True),
        CountryMetadata(iso3="ETH", data_available=True),
        CountryMetadata(iso3="UGA", data_available=False),
    ]
    sorted_records = aggregate_comparison.fn(records)

    assert len(sorted_records) == 3
    assert sorted_records[0].iso3 == "ETH"
    assert sorted_records[1].iso3 == "KEN"
    assert sorted_records[2].iso3 == "UGA"


def test_build_comparison_report() -> None:
    records = [
        CountryMetadata(iso3="ETH", title="Ethiopia 2020", popyear="2020", data_available=True),
        CountryMetadata(iso3="KEN", data_available=False),
    ]
    query = ComparisonQuery(iso3_codes=["ETH", "KEN"])

    report = build_comparison_report.fn(records, query)

    assert isinstance(report, ComparisonReport)
    assert report.countries_queried == 2
    assert report.countries_with_data == 1
    assert "ETH" in report.markdown
    assert "KEN" in report.markdown
    assert "Yes" in report.markdown
    assert "No" in report.markdown


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("cloud_worldpop_country_comparison.httpx.Client")
def test_flow_runs(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"data": [{"title": "Country 2020", "popyear": 2020, "doi": "10.xxx"}]}
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    query = ComparisonQuery(iso3_codes=["ETH", "KEN"])
    state = worldpop_country_comparison_flow(query=query, return_state=True)
    assert state.is_completed()
