"""Tests for the dhis2_worldbank_import deployment flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "deploy_dhis2_worldbank_import",
    Path(__file__).resolve().parent.parent / "deployments" / "dhis2_worldbank_import" / "flow.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["deploy_dhis2_worldbank_import"] = _mod
_spec.loader.exec_module(_mod)

from prefect_examples.dhis2 import Dhis2Client, Dhis2Credentials  # noqa: E402

CountryPopulation = _mod.CountryPopulation
OrgUnitMapping = _mod.OrgUnitMapping
DataValue = _mod.DataValue
ImportResult = _mod.ImportResult
fetch_population_data = _mod.fetch_population_data
resolve_org_units = _mod.resolve_org_units
build_data_values = _mod.build_data_values
import_data_values = _mod.import_data_values
dhis2_worldbank_import_flow = _mod.dhis2_worldbank_import_flow


@patch("httpx.Client")
def test_fetch_population_data(mock_client_cls: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"page": 1, "total": 2},
        [
            {
                "countryiso3code": "ETH",
                "country": {"value": "Ethiopia"},
                "date": "2023",
                "value": 126527060,
            },
            {
                "countryiso3code": "KEN",
                "country": {"value": "Kenya"},
                "date": "2023",
                "value": 55100586,
            },
        ],
    ]
    mock_response.raise_for_status = MagicMock()
    mock_client_cls.return_value.__enter__ = MagicMock(return_value=MagicMock())
    mock_client_cls.return_value.__enter__.return_value.get.return_value = mock_response
    mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

    result = fetch_population_data.fn(["ETH", "KEN"], 2023, 2023)
    assert len(result) == 2
    assert result[0].iso3 == "ETH"
    assert result[0].population == 126527060
    assert result[1].iso3 == "KEN"


def test_resolve_org_units() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_organisation_units_by_code.return_value = [
        {"id": "OU_ETH", "code": "ETH", "name": "Ethiopia"},
        {"id": "OU_KEN", "code": "KEN", "name": "Kenya"},
    ]

    result = resolve_org_units.fn(mock_client, ["ETH", "KEN"])
    assert len(result) == 2
    assert result["ETH"].org_unit_uid == "OU_ETH"
    assert result["KEN"].org_unit_name == "Kenya"


def test_build_data_values() -> None:
    populations = [
        CountryPopulation(iso3="ETH", year=2023, population=126527060),
        CountryPopulation(iso3="KEN", year=2023, population=55100586),
        CountryPopulation(iso3="MOZ", year=2023, population=33897354),
    ]
    org_unit_map = {
        "ETH": OrgUnitMapping(iso3="ETH", org_unit_uid="OU_ETH"),
        "KEN": OrgUnitMapping(iso3="KEN", org_unit_uid="OU_KEN"),
    }

    result = build_data_values.fn(populations, org_unit_map, "DE_POP")
    assert len(result) == 2
    assert result[0].dataElement == "DE_POP"
    assert result[0].orgUnit == "OU_ETH"
    assert result[0].period == "2023"
    assert result[0].value == "126527060"
    assert result[1].orgUnit == "OU_KEN"


def test_import_data_values() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.post_data_values.return_value = {
        "status": "SUCCESS",
        "importCount": {
            "imported": 2,
            "updated": 0,
            "ignored": 0,
            "deleted": 0,
        },
    }

    data_values = [
        DataValue(dataElement="DE_POP", period="2023", orgUnit="OU_ETH", value="126527060"),
        DataValue(dataElement="DE_POP", period="2023", orgUnit="OU_KEN", value="55100586"),
    ]

    result = import_data_values.fn(mock_client, data_values)
    assert isinstance(result, ImportResult)
    assert result.status == "SUCCESS"
    assert result.imported == 2
    assert result.total_submitted == 2


@patch("httpx.Client")
@patch.object(Dhis2Credentials, "get_client")
def test_flow_runs(mock_get_client: MagicMock, mock_httpx_client_cls: MagicMock) -> None:
    # Mock DHIS2 client
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_organisation_units_by_code.return_value = [
        {"id": "OU_ETH", "code": "ETH", "name": "Ethiopia"},
        {"id": "OU_KEN", "code": "KEN", "name": "Kenya"},
        {"id": "OU_MOZ", "code": "MOZ", "name": "Mozambique"},
    ]
    mock_client.post_data_values.return_value = {
        "status": "SUCCESS",
        "importCount": {
            "imported": 3,
            "updated": 0,
            "ignored": 0,
            "deleted": 0,
        },
    }
    mock_get_client.return_value = mock_client

    # Mock httpx
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"page": 1, "total": 3},
        [
            {
                "countryiso3code": "ETH",
                "country": {"value": "Ethiopia"},
                "date": "2023",
                "value": 126527060,
            },
            {
                "countryiso3code": "KEN",
                "country": {"value": "Kenya"},
                "date": "2023",
                "value": 55100586,
            },
            {
                "countryiso3code": "MOZ",
                "country": {"value": "Mozambique"},
                "date": "2023",
                "value": 33897354,
            },
        ],
    ]
    mock_response.raise_for_status = MagicMock()
    mock_httpx_client_cls.return_value.__enter__ = MagicMock(return_value=MagicMock())
    mock_httpx_client_cls.return_value.__enter__.return_value.get.return_value = mock_response
    mock_httpx_client_cls.return_value.__exit__ = MagicMock(return_value=False)

    state = dhis2_worldbank_import_flow(return_state=True)
    assert state.is_completed()
    result = state.result()
    assert isinstance(result, ImportResult)
    assert result.status == "SUCCESS"
    assert result.imported == 3
    assert result.total_submitted == 3
