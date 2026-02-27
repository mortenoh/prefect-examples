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

from prefect_dhis2 import Dhis2Client  # noqa: E402
from prefect_dhis2.credentials import Dhis2Credentials  # noqa: E402

PopulationQuery = _mod.PopulationQuery
CountryPopulation = _mod.CountryPopulation
OrgUnitMapping = _mod.OrgUnitMapping
PopulationReport = _mod.PopulationReport
ensure_dhis2_metadata = _mod.ensure_dhis2_metadata
fetch_population_data = _mod.fetch_population_data
resolve_org_units = _mod.resolve_org_units
build_report = _mod.build_report
dhis2_worldbank_import_flow = _mod.dhis2_worldbank_import_flow


SAMPLE_METADATA_RESPONSE = {
    "status": "OK",
    "stats": {"created": 0, "updated": 2, "deleted": 0, "ignored": 0, "total": 2},
}


def test_ensure_dhis2_metadata() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = [{"id": "ROOT_OU", "name": "Lao PDR"}]
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE

    result = ensure_dhis2_metadata.fn(mock_client)

    payload = mock_client.post_metadata.call_args[0][0]
    assert payload["dataElements"][0]["name"] == "Prefect - Population"
    assert payload["dataSets"][0]["periodType"] == "Yearly"
    assert payload["dataSets"][0]["organisationUnits"] == [{"id": "ROOT_OU"}]
    assert result["status"] == "OK"


@patch("httpx.Client")
def test_fetch_population_data(mock_client_cls: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"page": 1, "total": 2},
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
        ],
    ]
    mock_response.raise_for_status = MagicMock()
    mock_client_cls.return_value.__enter__ = MagicMock(return_value=MagicMock())
    mock_client_cls.return_value.__enter__.return_value.get.return_value = mock_response
    mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

    result = fetch_population_data.fn(["LAO"], 2022, 2023)
    assert len(result) == 2
    assert result[0].iso3 == "LAO"
    assert result[0].population == 7633779


def test_resolve_org_units() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_organisation_units_by_code.return_value = [
        {"id": "OU_LAO", "code": "LAO", "name": "Lao PDR"},
    ]

    result = resolve_org_units.fn(mock_client, ["LAO"])
    assert len(result) == 1
    assert result["LAO"].org_unit_uid == "OU_LAO"
    assert result["LAO"].org_unit_name == "Lao PDR"


def test_build_report() -> None:
    populations = [
        CountryPopulation(iso3="LAO", country_name="Lao PDR", year=2023, population=7633779),
        CountryPopulation(iso3="LAO", country_name="Lao PDR", year=2022, population=7529475),
    ]
    org_unit_map = {
        "LAO": OrgUnitMapping(iso3="LAO", org_unit_uid="OU_LAO", org_unit_name="Lao PDR"),
    }
    query = PopulationQuery(iso3_codes=["LAO"], start_year=2022, end_year=2023)

    report = build_report.fn("https://dhis2.example.org", query, populations, org_unit_map)

    assert isinstance(report, PopulationReport)
    assert report.record_count == 2
    assert report.org_units_resolved == 1
    assert "Lao PDR" in report.markdown
    assert "7,633,779" in report.markdown


@patch("httpx.Client")
@patch.object(Dhis2Credentials, "get_client")
def test_flow_runs(mock_get_client: MagicMock, mock_httpx_client_cls: MagicMock) -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = [{"id": "ROOT_OU", "name": "Lao PDR"}]
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE
    mock_client.fetch_organisation_units_by_code.return_value = [
        {"id": "OU_LAO", "code": "LAO", "name": "Lao PDR"},
    ]
    mock_get_client.return_value = mock_client

    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"page": 1, "total": 2},
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
        ],
    ]
    mock_response.raise_for_status = MagicMock()
    mock_httpx_client_cls.return_value.__enter__ = MagicMock(return_value=MagicMock())
    mock_httpx_client_cls.return_value.__enter__.return_value.get.return_value = mock_response
    mock_httpx_client_cls.return_value.__exit__ = MagicMock(return_value=False)

    state = dhis2_worldbank_import_flow(return_state=True)
    assert state.is_completed()
    report = state.result()
    assert isinstance(report, PopulationReport)
    assert report.record_count == 2
    assert report.org_units_resolved == 1
