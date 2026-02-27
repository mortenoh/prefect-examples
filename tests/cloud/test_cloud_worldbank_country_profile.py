"""Tests for World Bank Country Profile flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "cloud_worldbank_country_profile",
    Path(__file__).resolve().parent.parent.parent / "flows" / "cloud" / "cloud_worldbank_country_profile.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["cloud_worldbank_country_profile"] = _mod
_spec.loader.exec_module(_mod)

ProfileQuery = _mod.ProfileQuery
IndicatorValue = _mod.IndicatorValue
CountryProfile = _mod.CountryProfile
DEFAULT_INDICATORS = _mod.DEFAULT_INDICATORS
fetch_indicator = _mod.fetch_indicator
build_country_profile = _mod.build_country_profile
worldbank_country_profile_flow = _mod.worldbank_country_profile_flow


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_profile_query_defaults() -> None:
    q = ProfileQuery()
    assert q.iso3 == "ETH"
    assert q.year == 2023
    assert q.indicators == DEFAULT_INDICATORS


def test_indicator_value_model() -> None:
    iv = IndicatorValue(
        indicator_code="SP.POP.TOTL",
        indicator_name="Population, total",
        value=120_000_000.0,
        year=2023,
    )
    assert iv.indicator_code == "SP.POP.TOTL"
    assert iv.indicator_name == "Population, total"
    assert iv.value == 120_000_000.0
    assert iv.year == 2023


# ---------------------------------------------------------------------------
# Task tests
# ---------------------------------------------------------------------------


@patch("cloud_worldbank_country_profile.httpx.Client")
def test_fetch_indicator(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {"page": 1, "pages": 1, "per_page": 50, "total": 1},
        [
            {
                "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
                "country": {"id": "ET", "value": "Ethiopia"},
                "countryiso3code": "ETH",
                "date": "2023",
                "value": 126527060,
            }
        ],
    ]
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = fetch_indicator.fn("ETH", "SP.POP.TOTL", 2023)

    assert result.indicator_code == "SP.POP.TOTL"
    assert result.indicator_name == "Population, total"
    assert result.value == 126527060.0
    assert result.year == 2023


@patch("cloud_worldbank_country_profile.httpx.Client")
def test_fetch_indicator_null_value(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {"page": 1, "pages": 1, "per_page": 50, "total": 1},
        [
            {
                "indicator": {"id": "SE.ADT.LITR.ZS", "value": "Literacy rate"},
                "country": {"id": "ET", "value": "Ethiopia"},
                "countryiso3code": "ETH",
                "date": "2023",
                "value": None,
            }
        ],
    ]
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = fetch_indicator.fn("ETH", "SE.ADT.LITR.ZS", 2023)

    assert result.indicator_code == "SE.ADT.LITR.ZS"
    assert result.value is None


def test_build_country_profile() -> None:
    indicators = [
        IndicatorValue(
            indicator_code="SP.POP.TOTL",
            indicator_name="Population, total",
            value=126_527_060.0,
            year=2023,
        ),
        IndicatorValue(
            indicator_code="NY.GDP.MKTP.CD",
            indicator_name="GDP (current US$)",
            value=155_800_000_000.0,
            year=2023,
        ),
        IndicatorValue(
            indicator_code="SE.ADT.LITR.ZS",
            indicator_name="Literacy rate",
            value=None,
            year=2023,
        ),
    ]
    result = build_country_profile.fn("ETH", 2023, indicators)

    assert result.iso3 == "ETH"
    assert result.year == 2023
    assert "Country Profile: ETH (2023)" in result.markdown
    assert "Population, total" in result.markdown
    assert "126,527,060" in result.markdown
    assert "$155.8B" in result.markdown
    assert "N/A" in result.markdown
    assert "Indicators with data" in result.markdown
    assert "2/3" in result.markdown


def test_build_country_profile_formats_values() -> None:
    indicators = [
        IndicatorValue(
            indicator_code="SE.ADT.LITR.ZS",
            indicator_name="Literacy rate",
            value=72.3,
            year=2023,
        ),
        IndicatorValue(
            indicator_code="NY.GDP.MKTP.CD",
            indicator_name="GDP (current US$)",
            value=3_200_000_000_000.0,
            year=2023,
        ),
        IndicatorValue(
            indicator_code="SP.POP.TOTL",
            indicator_name="Population, total",
            value=45_000_000.0,
            year=2023,
        ),
        IndicatorValue(
            indicator_code="SH.DYN.MORT",
            indicator_name="Under-5 mortality rate",
            value=38.2,
            year=2023,
        ),
        IndicatorValue(
            indicator_code="SP.DYN.LE00.IN",
            indicator_name="Life expectancy",
            value=None,
            year=2023,
        ),
    ]
    result = build_country_profile.fn("KEN", 2023, indicators)

    # Percentage (.ZS suffix)
    assert "72.3%" in result.markdown
    # Billions
    assert "$3200.0B" in result.markdown
    # Millions (plain commas)
    assert "45,000,000" in result.markdown
    # Small number
    assert "38.2" in result.markdown
    # None value
    assert "N/A" in result.markdown


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("cloud_worldbank_country_profile.httpx.Client")
def test_flow_runs(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {"page": 1, "pages": 1, "per_page": 50, "total": 1},
        [
            {
                "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
                "country": {"id": "ET", "value": "Ethiopia"},
                "countryiso3code": "ETH",
                "date": "2023",
                "value": 126527060,
            }
        ],
    ]
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    query = ProfileQuery(iso3="ETH", year=2023, indicators=["SP.POP.TOTL"])
    state = worldbank_country_profile_flow(query=query, return_state=True)
    assert state.is_completed()
