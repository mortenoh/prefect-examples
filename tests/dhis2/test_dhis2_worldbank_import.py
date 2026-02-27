"""Tests for DHIS2 World Bank Population Import flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

_spec = importlib.util.spec_from_file_location(
    "dhis2_worldbank_import",
    Path(__file__).resolve().parent.parent.parent / "flows" / "dhis2" / "dhis2_worldbank_import.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["dhis2_worldbank_import"] = _mod
_spec.loader.exec_module(_mod)

from prefect_examples.dhis2 import Dhis2Client, Dhis2Credentials  # noqa: E402

ImportQuery = _mod.ImportQuery
CountryPopulation = _mod.CountryPopulation
OrgUnitMapping = _mod.OrgUnitMapping
DataValue = _mod.DataValue
ImportResult = _mod.ImportResult
fetch_population_data = _mod.fetch_population_data
resolve_org_units = _mod.resolve_org_units
build_data_values = _mod.build_data_values
import_data_values = _mod.import_data_values
dhis2_worldbank_import_flow = _mod.dhis2_worldbank_import_flow

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

SAMPLE_WB_RESPONSE = [
    {"page": 1, "pages": 1, "per_page": 10, "total": 4},
    [
        {
            "countryiso3code": "ETH",
            "country": {"value": "Ethiopia"},
            "date": "2023",
            "value": 126527060,
            "indicator": {"id": "SP.POP.TOTL"},
        },
        {
            "countryiso3code": "ETH",
            "country": {"value": "Ethiopia"},
            "date": "2022",
            "value": 123379924,
            "indicator": {"id": "SP.POP.TOTL"},
        },
        {
            "countryiso3code": "KEN",
            "country": {"value": "Kenya"},
            "date": "2023",
            "value": 55100586,
            "indicator": {"id": "SP.POP.TOTL"},
        },
        {
            "countryiso3code": "KEN",
            "country": {"value": "Kenya"},
            "date": "2022",
            "value": None,
        },
    ],
]

SAMPLE_ORG_UNITS = [
    {"id": "OU_ETH", "code": "ETH", "name": "Ethiopia"},
    {"id": "OU_KEN", "code": "KEN", "name": "Kenya"},
]

SAMPLE_IMPORT_RESPONSE = {
    "status": "SUCCESS",
    "importCount": {"imported": 3, "updated": 0, "ignored": 0, "deleted": 0},
    "description": "Import complete",
}


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_import_query_requires_data_element_uid() -> None:
    with pytest.raises(ValidationError):
        ImportQuery(iso3_codes=["ETH"], start_year=2020, end_year=2023)


def test_import_query_valid() -> None:
    q = ImportQuery(iso3_codes=["ETH"], start_year=2020, end_year=2023, data_element_uid="abc123")
    assert q.data_element_uid == "abc123"
    assert q.iso3_codes == ["ETH"]


def test_data_value_camel_case() -> None:
    dv = DataValue(dataElement="DE1", period="2023", orgUnit="OU1", value="100")
    dumped = dv.model_dump()
    assert "dataElement" in dumped
    assert "orgUnit" in dumped


# ---------------------------------------------------------------------------
# Task tests
# ---------------------------------------------------------------------------


@patch("dhis2_worldbank_import.httpx.Client")
def test_fetch_population_data(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = SAMPLE_WB_RESPONSE
    mock_resp.raise_for_status = MagicMock()

    mock_http = MagicMock()
    mock_http.get.return_value = mock_resp
    mock_http.__enter__ = MagicMock(return_value=mock_http)
    mock_http.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_http

    results = fetch_population_data.fn(["ETH", "KEN"], 2022, 2023)

    assert len(results) == 3  # one null value filtered out
    assert all(isinstance(r, CountryPopulation) for r in results)
    assert results[0].iso3 == "ETH"
    assert results[0].population == 126527060


@patch("dhis2_worldbank_import.httpx.Client")
def test_fetch_population_data_empty(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = [{"page": 1}, None]
    mock_resp.raise_for_status = MagicMock()

    mock_http = MagicMock()
    mock_http.get.return_value = mock_resp
    mock_http.__enter__ = MagicMock(return_value=mock_http)
    mock_http.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_http

    results = fetch_population_data.fn(["ZZZ"], 2023, 2023)
    assert results == []


def test_resolve_org_units() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_organisation_units_by_code.return_value = SAMPLE_ORG_UNITS

    mapping = resolve_org_units.fn(mock_client, ["ETH", "KEN", "MOZ"])

    assert len(mapping) == 2
    assert "ETH" in mapping
    assert mapping["ETH"].org_unit_uid == "OU_ETH"
    assert mapping["KEN"].org_unit_name == "Kenya"
    # MOZ is missing -- should log warning but not raise


def test_resolve_org_units_all_found() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_organisation_units_by_code.return_value = SAMPLE_ORG_UNITS

    mapping = resolve_org_units.fn(mock_client, ["ETH", "KEN"])
    assert len(mapping) == 2


def test_build_data_values() -> None:
    populations = [
        CountryPopulation(iso3="ETH", country_name="Ethiopia", year=2023, population=126527060),
        CountryPopulation(iso3="KEN", country_name="Kenya", year=2023, population=55100586),
        CountryPopulation(iso3="MOZ", country_name="Mozambique", year=2023, population=33897354),
    ]
    org_map = {
        "ETH": OrgUnitMapping(iso3="ETH", org_unit_uid="OU_ETH", org_unit_name="Ethiopia"),
        "KEN": OrgUnitMapping(iso3="KEN", org_unit_uid="OU_KEN", org_unit_name="Kenya"),
    }

    values = build_data_values.fn(populations, org_map, "DE_POP")

    assert len(values) == 2  # MOZ skipped
    assert values[0].dataElement == "DE_POP"
    assert values[0].orgUnit == "OU_ETH"
    assert values[0].period == "2023"
    assert values[0].value == "126527060"


def test_build_data_values_empty_map() -> None:
    populations = [
        CountryPopulation(iso3="ETH", country_name="Ethiopia", year=2023, population=100),
    ]
    values = build_data_values.fn(populations, {}, "DE_POP")
    assert values == []


def test_import_data_values() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE

    data_values = [
        DataValue(dataElement="DE1", period="2023", orgUnit="OU1", value="100"),
        DataValue(dataElement="DE1", period="2023", orgUnit="OU2", value="200"),
        DataValue(dataElement="DE1", period="2022", orgUnit="OU1", value="90"),
    ]

    result = import_data_values.fn(mock_client, data_values)

    assert isinstance(result, ImportResult)
    assert result.status == "SUCCESS"
    assert result.imported == 3
    assert result.updated == 0
    assert result.total_submitted == 3
    assert "Import Result" in result.markdown


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("dhis2_worldbank_import.httpx.Client")
@patch.object(Dhis2Credentials, "get_client")
def test_flow_runs(mock_get_client: MagicMock, mock_httpx_cls: MagicMock) -> None:
    # Mock DHIS2 client
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_organisation_units_by_code.return_value = SAMPLE_ORG_UNITS
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE
    mock_get_client.return_value = mock_client

    # Mock World Bank httpx.Client
    mock_wb_resp = MagicMock()
    mock_wb_resp.json.return_value = SAMPLE_WB_RESPONSE
    mock_wb_resp.raise_for_status = MagicMock()

    mock_http = MagicMock()
    mock_http.get.return_value = mock_wb_resp
    mock_http.__enter__ = MagicMock(return_value=mock_http)
    mock_http.__exit__ = MagicMock(return_value=False)
    mock_httpx_cls.return_value = mock_http

    query = ImportQuery(
        iso3_codes=["ETH", "KEN"],
        start_year=2022,
        end_year=2023,
        data_element_uid="DE_POP",
    )

    state = dhis2_worldbank_import_flow(query=query, return_state=True)
    assert state.is_completed()
