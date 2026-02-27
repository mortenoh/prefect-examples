"""Tests for DHIS2 World Bank Health Indicators Import flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

_spec = importlib.util.spec_from_file_location(
    "dhis2_worldbank_health_import",
    Path(__file__).resolve().parent.parent.parent / "flows" / "dhis2" / "dhis2_worldbank_health_import.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["dhis2_worldbank_health_import"] = _mod
_spec.loader.exec_module(_mod)

from prefect_dhis2 import Dhis2Client  # noqa: E402
from prefect_dhis2.credentials import Dhis2Credentials  # noqa: E402

HealthQuery = _mod.HealthQuery
IndicatorConfig = _mod.IndicatorConfig
IndicatorValue = _mod.IndicatorValue
OrgUnit = _mod.OrgUnit
DataValue = _mod.DataValue
ImportResult = _mod.ImportResult
INDICATORS = _mod.INDICATORS
ensure_dhis2_metadata = _mod.ensure_dhis2_metadata
fetch_wb_data = _mod.fetch_wb_data
build_data_values = _mod.build_data_values
import_to_dhis2 = _mod.import_to_dhis2
dhis2_worldbank_health_import_flow = _mod.dhis2_worldbank_health_import_flow

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

SAMPLE_WB_RESPONSE = [
    {"page": 1, "pages": 1, "per_page": 10, "total": 2},
    [
        {
            "countryiso3code": "LAO",
            "country": {"value": "Lao PDR"},
            "date": "2023",
            "value": 44.3,
        },
        {
            "countryiso3code": "LAO",
            "country": {"value": "Lao PDR"},
            "date": "2022",
            "value": 46.1,
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
    "stats": {"created": 11, "updated": 0, "deleted": 0, "ignored": 0, "total": 11},
}

SAMPLE_IMPORT_RESPONSE = {
    "status": "SUCCESS",
    "importCount": {"imported": 2, "updated": 0, "ignored": 0, "deleted": 0},
}


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_health_query_valid() -> None:
    q = HealthQuery(iso3_codes=["LAO"], start_year=2020, end_year=2023)
    assert q.iso3_codes == ["LAO"]


def test_health_query_requires_iso3() -> None:
    with pytest.raises(ValidationError):
        HealthQuery(start_year=2020, end_year=2023)


def test_indicators_count() -> None:
    assert len(INDICATORS) == 10


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
    assert len(payload["dataElements"]) == 10
    assert len(payload["dataSets"]) == 1
    assert payload["dataSets"][0]["name"] == "PR - World Bank Health Indicators"
    assert payload["dataSets"][0]["periodType"] == "Yearly"
    assert len(payload["dataSets"][0]["dataSetElements"]) == 10
    assert payload["dataSets"][0]["organisationUnits"] == [{"id": "ROOT_OU"}]


def test_ensure_dhis2_metadata_no_level1() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = []

    with pytest.raises(ValueError, match="No level-1"):
        ensure_dhis2_metadata.fn(mock_client)


@patch("dhis2_worldbank_health_import.httpx.Client")
def test_fetch_wb_data(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = SAMPLE_WB_RESPONSE
    mock_resp.raise_for_status = MagicMock()

    mock_http = MagicMock()
    mock_http.get.return_value = mock_resp
    mock_http.__enter__ = MagicMock(return_value=mock_http)
    mock_http.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_http

    indicator = INDICATORS[0]
    results = fetch_wb_data.fn(indicator, ["LAO"], 2021, 2023)

    assert len(results) == 2  # null filtered out
    assert results[0].indicator == "SH.DYN.MORT"
    assert results[0].de_uid == "PfWbU5Mort1"
    assert results[0].value == 44.3


def test_build_data_values() -> None:
    values = [
        IndicatorValue(indicator="SH.DYN.MORT", de_uid="PfWbU5Mort1", year=2023, value=44.3),
        IndicatorValue(indicator="SH.DYN.MORT", de_uid="PfWbU5Mort1", year=2022, value=46.1),
    ]
    org_unit = OrgUnit(id="ROOT_OU", name="Lao PDR")

    data_values = build_data_values.fn(org_unit, values)

    assert len(data_values) == 2
    assert data_values[0].dataElement == "PfWbU5Mort1"
    assert data_values[0].orgUnit == "ROOT_OU"
    assert data_values[0].period == "2023"
    assert data_values[0].value == "44.3"


def test_build_data_values_empty() -> None:
    org_unit = OrgUnit(id="ROOT_OU", name="Lao PDR")

    data_values = build_data_values.fn(org_unit, [])

    assert data_values == []


def test_import_to_dhis2() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE

    org_unit = OrgUnit(id="ROOT_OU", name="Lao PDR")
    data_values = [
        DataValue(dataElement="PfWbU5Mort1", period="2023", orgUnit="ROOT_OU", value="44.3"),
        DataValue(dataElement="PfWbU5Mort1", period="2022", orgUnit="ROOT_OU", value="46.1"),
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


@patch("dhis2_worldbank_health_import.httpx.Client")
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

    state = dhis2_worldbank_health_import_flow(return_state=True)
    assert state.is_completed()
    result = state.result()
    assert isinstance(result, ImportResult)
    assert result.imported == 2
    assert result.org_unit.id == "ROOT_OU"
