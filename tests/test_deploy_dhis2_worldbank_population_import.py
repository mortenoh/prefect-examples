"""Tests for the dhis2_worldbank_population_import deployment flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "deploy_dhis2_worldbank_population_import",
    Path(__file__).resolve().parent.parent / "deployments" / "dhis2_worldbank_population_import" / "flow.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["deploy_dhis2_worldbank_population_import"] = _mod
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
dhis2_worldbank_population_import_flow = _mod.dhis2_worldbank_population_import_flow

SAMPLE_LEVEL1_OU = [{"id": "ROOT_OU", "name": "Root"}]
SAMPLE_METADATA_RESPONSE = {
    "status": "OK",
    "stats": {"created": 2, "updated": 0, "deleted": 0, "ignored": 0, "total": 2},
}
SAMPLE_IMPORT_RESPONSE = {
    "status": "SUCCESS",
    "importCount": {"imported": 1, "updated": 0, "ignored": 0, "deleted": 0},
}


def test_ensure_dhis2_metadata() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_LEVEL1_OU
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE

    org_unit = ensure_dhis2_metadata.fn(mock_client)

    assert org_unit.id == "ROOT_OU"
    payload = mock_client.post_metadata.call_args[0][0]
    assert payload["dataElements"][0]["name"] == "Prefect - Population"
    assert payload["dataSets"][0]["periodType"] == "Yearly"
    assert payload["dataSets"][0]["organisationUnits"] == [{"id": "ROOT_OU"}]


@patch("deploy_dhis2_worldbank_population_import.httpx.Client")
def test_fetch_population_data(mock_client_cls: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"page": 1},
        [
            {"countryiso3code": "LAO", "country": {"value": "Lao PDR"}, "date": "2023", "value": 7633779},
        ],
    ]
    mock_response.raise_for_status = MagicMock()
    mock_client_cls.return_value.__enter__ = MagicMock(return_value=MagicMock())
    mock_client_cls.return_value.__enter__.return_value.get.return_value = mock_response
    mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

    result = fetch_population_data.fn(["LAO"], 2023, 2023)
    assert len(result) == 1
    assert result[0].population == 7633779


def test_build_data_values() -> None:
    populations = [CountryPopulation(iso3="LAO", country_name="Lao PDR", year=2023, population=7633779)]
    org_unit = OrgUnit(id="ROOT_OU", name="Root")

    values = build_data_values.fn(org_unit, populations)

    assert len(values) == 1
    assert values[0].dataElement == "PfPopTotal1"
    assert values[0].orgUnit == "ROOT_OU"
    assert values[0].period == "2023"
    assert values[0].value == "7633779"


def test_import_to_dhis2() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE

    org_unit = OrgUnit(id="ROOT_OU", name="Root")
    data_values = [
        DataValue(dataElement="PfPopTotal1", period="2023", orgUnit="ROOT_OU", value="7633779"),
    ]

    result = import_to_dhis2.fn(mock_client, "https://dhis2.example.org", org_unit, data_values)

    assert isinstance(result, ImportResult)
    assert result.imported == 1
    assert result.total == 1


@patch("deploy_dhis2_worldbank_population_import.httpx.Client")
@patch.object(Dhis2Credentials, "get_client")
def test_flow_runs(mock_get_client: MagicMock, mock_httpx_client_cls: MagicMock) -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_LEVEL1_OU
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE
    mock_get_client.return_value = mock_client

    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"page": 1},
        [{"countryiso3code": "LAO", "country": {"value": "Lao PDR"}, "date": "2023", "value": 7633779}],
    ]
    mock_response.raise_for_status = MagicMock()
    mock_httpx_client_cls.return_value.__enter__ = MagicMock(return_value=MagicMock())
    mock_httpx_client_cls.return_value.__enter__.return_value.get.return_value = mock_response
    mock_httpx_client_cls.return_value.__exit__ = MagicMock(return_value=False)

    state = dhis2_worldbank_population_import_flow(return_state=True)
    assert state.is_completed()
    result = state.result()
    assert isinstance(result, ImportResult)
    assert result.org_unit.id == "ROOT_OU"
