"""Tests for WorldPop Country Population Report flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "cloud_worldpop_country_report",
    Path(__file__).resolve().parent.parent.parent / "flows" / "cloud" / "cloud_worldpop_country_report.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["cloud_worldpop_country_report"] = _mod
_spec.loader.exec_module(_mod)

CountryReportQuery = _mod.CountryReportQuery
CountryPopulationInfo = _mod.CountryPopulationInfo
PopulationReport = _mod.PopulationReport
fetch_country_data = _mod.fetch_country_data
fetch_population = _mod.fetch_population
transform_results = _mod.transform_results
build_report = _mod.build_report
send_slack_notification = _mod.send_slack_notification
worldpop_country_report_flow = _mod.worldpop_country_report_flow


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_country_report_query_defaults() -> None:
    q = CountryReportQuery(iso3_codes=["ETH", "KEN"])
    assert q.dataset == "pop"
    assert q.subdataset == "wpgp"
    assert q.year == 2024


def test_country_population_info_model() -> None:
    c = CountryPopulationInfo(iso3="ETH", years_available=21, latest_year=2020, earliest_year=2000)
    assert c.years_available == 21


def test_country_population_info_no_data() -> None:
    c = CountryPopulationInfo(iso3="XXX")
    assert c.years_available == 0
    assert c.latest_year is None


# ---------------------------------------------------------------------------
# Task tests
# ---------------------------------------------------------------------------


@patch("cloud_worldpop_country_report.httpx.Client")
def test_fetch_country_data(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "data": [
            {"title": "ETH 2019", "popyear": 2019, "doi": "10.aaa"},
            {"title": "ETH 2020", "popyear": 2020, "doi": "10.bbb"},
            {"title": "ETH 2018", "popyear": 2018, "doi": "10.ccc"},
        ]
    }
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = fetch_country_data.fn("ETH", "pop", "wpgp")

    assert result.iso3 == "ETH"
    assert result.years_available == 3
    assert result.earliest_year == 2018
    assert result.latest_year == 2020
    assert result.doi == "10.bbb"


@patch("cloud_worldpop_country_report.httpx.Client")
def test_fetch_country_data_empty(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"data": []}
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = fetch_country_data.fn("XXX", "pop", "wpgp")

    assert result.years_available == 0
    assert result.latest_year is None


@patch("cloud_worldpop_country_report.httpx.Client")
def test_fetch_population(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {"page": 1, "pages": 1, "per_page": 2, "total": 2},
        [
            {"countryiso3code": "ETH", "value": 132059767},
            {"countryiso3code": "KEN", "value": 55100586},
        ],
    ]
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = fetch_population.fn(["ETH", "KEN"], 2024)

    assert result == {"ETH": 132059767, "KEN": 55100586}


@patch("cloud_worldpop_country_report.httpx.Client")
def test_fetch_population_empty_response(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = [{"page": 1, "pages": 0, "per_page": 0, "total": 0}]
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = fetch_population.fn(["XXX"], 2024)

    assert result == {}


def test_transform_results() -> None:
    countries = [
        CountryPopulationInfo(iso3="ETH", years_available=5, population=120_000_000),
        CountryPopulationInfo(iso3="KEN", years_available=10, population=55_000_000),
        CountryPopulationInfo(iso3="TZA", years_available=3, population=65_000_000),
    ]
    result = transform_results.fn(countries)

    assert len(result) == 3
    assert result[0].iso3 == "ETH"  # sorted by population desc
    assert result[1].iso3 == "TZA"
    assert result[2].iso3 == "KEN"


def test_build_report() -> None:
    countries = [
        CountryPopulationInfo(
            iso3="ETH",
            years_available=21,
            latest_year=2020,
            earliest_year=2000,
            title="Ethiopia",
            population=120_000_000,
        ),
        CountryPopulationInfo(
            iso3="KEN",
            years_available=21,
            latest_year=2020,
            earliest_year=2000,
            title="Kenya",
            population=55_000_000,
        ),
    ]
    markdown = build_report.fn(countries)

    assert "ETH" in markdown
    assert "KEN" in markdown
    assert "2000-2020" in markdown
    assert "Countries with data" in markdown
    assert "120,000,000" in markdown
    assert "Total population" in markdown
    assert "175,000,000" in markdown  # total population


@patch.dict("os.environ", {"SLACK_WEBHOOK_URL": ""}, clear=False)
def test_send_slack_notification_no_url() -> None:
    countries = [CountryPopulationInfo(iso3="ETH", years_available=21, latest_year=2020, earliest_year=2000)]
    result = send_slack_notification.fn(countries)
    assert result is False


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("cloud_worldpop_country_report.send_slack_notification", return_value=False)
@patch("cloud_worldpop_country_report.httpx.Client")
def test_flow_runs(mock_client_cls: MagicMock, _mock_slack: MagicMock) -> None:
    worldpop_resp = MagicMock()
    worldpop_resp.json.return_value = {
        "data": [
            {"title": "Country 2020", "popyear": 2020, "doi": "10.xxx"},
            {"title": "Country 2019", "popyear": 2019, "doi": "10.yyy"},
        ]
    }
    worldbank_resp = MagicMock()
    worldbank_resp.json.return_value = [
        {"page": 1, "pages": 1, "per_page": 2, "total": 2},
        [
            {"countryiso3code": "ETH", "value": 132059767},
            {"countryiso3code": "KEN", "value": 55100586},
        ],
    ]
    mock_client = MagicMock()
    mock_client.get.side_effect = lambda url, **kwargs: worldbank_resp if "worldbank" in url else worldpop_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    query = CountryReportQuery(iso3_codes=["ETH", "KEN"])
    state = worldpop_country_report_flow(query=query, return_state=True)
    assert state.is_completed()
