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

from prefect_dhis2 import Dhis2Client  # noqa: E402
from prefect_dhis2.credentials import Dhis2Credentials  # noqa: E402

PopulationQuery = _mod.PopulationQuery
CountryPopulation = _mod.CountryPopulation
OrgUnit = _mod.OrgUnit
DataValue = _mod.DataValue
ImportResult = _mod.ImportResult
ensure_dhis2_metadata = _mod.ensure_dhis2_metadata
fetch_population_data = _mod.fetch_population_data
build_data_values = _mod.build_data_values
import_to_dhis2 = _mod.import_to_dhis2
dhis2_worldbank_import_flow = _mod.dhis2_worldbank_import_flow

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

SAMPLE_WB_RESPONSE = [
    {"page": 1, "pages": 1, "per_page": 10, "total": 3},
    [
        {
            "countryiso3code": "LAO",
            "country": {"value": "Lao PDR"},
            "date": "2023",
            "value": 7633779,
        },
        {
            "countryiso3code": "LAO",
            "country": {"value": "Lao PDR"},
            "date": "2022",
            "value": 7529475,
        },
        {
            "countryiso3code": "LAO",
            "country": {"value": "Lao PDR"},
            "date": "2021",
            "value": None,
        },
    ],
]

SAMPLE_LEVEL1_OU = [{"id": "ROOT_OU", "name": "Lao PDR"}]

SAMPLE_METADATA_RESPONSE = {
    "status": "OK",
    "stats": {"created": 2, "updated": 0, "deleted": 0, "ignored": 0, "total": 2},
}

SAMPLE_IMPORT_RESPONSE = {
    "status": "SUCCESS",
    "importCount": {"imported": 2, "updated": 0, "ignored": 0, "deleted": 0},
}


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_population_query_valid() -> None:
    q = PopulationQuery(iso3_codes=["LAO"], start_year=2020, end_year=2023)
    assert q.iso3_codes == ["LAO"]


def test_population_query_requires_iso3() -> None:
    with pytest.raises(ValidationError):
        PopulationQuery(start_year=2020, end_year=2023)


# ---------------------------------------------------------------------------
# Task tests
# ---------------------------------------------------------------------------


def test_ensure_dhis2_metadata() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_LEVEL1_OU
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE

    org_unit = ensure_dhis2_metadata.fn(mock_client)

    assert isinstance(org_unit, OrgUnit)
    assert org_unit.id == "ROOT_OU"
    assert org_unit.name == "Lao PDR"
    mock_client.post_metadata.assert_called_once()
    payload = mock_client.post_metadata.call_args[0][0]
    assert payload["dataElements"][0]["name"] == "Prefect - Population"
    assert payload["dataSets"][0]["periodType"] == "Yearly"
    assert payload["dataSets"][0]["organisationUnits"] == [{"id": "ROOT_OU"}]


def test_ensure_dhis2_metadata_no_level1() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = []

    with pytest.raises(ValueError, match="No level-1"):
        ensure_dhis2_metadata.fn(mock_client)


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

    results = fetch_population_data.fn(["LAO"], 2021, 2023)

    assert len(results) == 2  # null filtered out
    assert results[0].iso3 == "LAO"
    assert results[0].population == 7633779


def test_build_data_values() -> None:
    populations = [
        CountryPopulation(iso3="LAO", country_name="Lao PDR", year=2023, population=7633779),
        CountryPopulation(iso3="LAO", country_name="Lao PDR", year=2022, population=7529475),
    ]
    org_unit = OrgUnit(id="ROOT_OU", name="Lao PDR")

    values = build_data_values.fn(org_unit, populations)

    assert len(values) == 2
    assert values[0].dataElement == "PfPopTotal1"
    assert values[0].orgUnit == "ROOT_OU"
    assert values[0].period == "2023"
    assert values[0].value == "7633779"


def test_build_data_values_empty() -> None:
    org_unit = OrgUnit(id="ROOT_OU", name="Lao PDR")

    values = build_data_values.fn(org_unit, [])

    assert values == []


def test_import_to_dhis2() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE

    org_unit = OrgUnit(id="ROOT_OU", name="Lao PDR")
    data_values = [
        DataValue(dataElement="PfPopTotal1", period="2023", orgUnit="ROOT_OU", value="7633779"),
        DataValue(dataElement="PfPopTotal1", period="2022", orgUnit="ROOT_OU", value="7529475"),
    ]

    result = import_to_dhis2.fn(mock_client, "https://dhis2.example.org", org_unit, data_values)

    assert isinstance(result, ImportResult)
    assert result.imported == 2
    assert result.total == 2
    mock_client.post_data_values.assert_called_once()
    payload = mock_client.post_data_values.call_args[0][0]
    assert len(payload["dataValues"]) == 2


def test_import_to_dhis2_empty() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    org_unit = OrgUnit(id="ROOT_OU", name="Lao PDR")

    result = import_to_dhis2.fn(mock_client, "https://dhis2.example.org", org_unit, [])

    assert result.total == 0
    mock_client.post_data_values.assert_not_called()


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("dhis2_worldbank_import.httpx.Client")
@patch.object(Dhis2Credentials, "get_client")
def test_flow_runs(mock_get_client: MagicMock, mock_httpx_cls: MagicMock) -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_LEVEL1_OU
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE
    mock_get_client.return_value = mock_client

    mock_wb_resp = MagicMock()
    mock_wb_resp.json.return_value = SAMPLE_WB_RESPONSE
    mock_wb_resp.raise_for_status = MagicMock()

    mock_http = MagicMock()
    mock_http.get.return_value = mock_wb_resp
    mock_http.__enter__ = MagicMock(return_value=mock_http)
    mock_http.__exit__ = MagicMock(return_value=False)
    mock_httpx_cls.return_value = mock_http

    state = dhis2_worldbank_import_flow(return_state=True)
    assert state.is_completed()
    result = state.result()
    assert isinstance(result, ImportResult)
    assert result.imported == 2
    assert result.org_unit.id == "ROOT_OU"
