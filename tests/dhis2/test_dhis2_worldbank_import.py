"""Tests for DHIS2 World Bank Population Report flow."""

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
OrgUnitMapping = _mod.OrgUnitMapping
PopulationReport = _mod.PopulationReport
Dhis2DataElement = _mod.Dhis2DataElement
Dhis2DataSet = _mod.Dhis2DataSet
ensure_dhis2_metadata = _mod.ensure_dhis2_metadata
fetch_population_data = _mod.fetch_population_data
resolve_org_units = _mod.resolve_org_units
build_report = _mod.build_report
dhis2_worldbank_import_flow = _mod.dhis2_worldbank_import_flow

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

SAMPLE_WB_RESPONSE = [
    {"page": 1, "pages": 1, "per_page": 10, "total": 4},
    [
        {
            "countryiso3code": "LAO",
            "country": {"value": "Lao PDR"},
            "date": "2023",
            "value": 7633779,
            "indicator": {"id": "SP.POP.TOTL"},
        },
        {
            "countryiso3code": "LAO",
            "country": {"value": "Lao PDR"},
            "date": "2022",
            "value": 7529475,
            "indicator": {"id": "SP.POP.TOTL"},
        },
        {
            "countryiso3code": "LAO",
            "country": {"value": "Lao PDR"},
            "date": "2021",
            "value": 7425057,
            "indicator": {"id": "SP.POP.TOTL"},
        },
        {
            "countryiso3code": "LAO",
            "country": {"value": "Lao PDR"},
            "date": "2020",
            "value": None,
        },
    ],
]

SAMPLE_ORG_UNITS = [
    {"id": "OU_LAO", "code": "LAO", "name": "Lao PDR"},
]

SAMPLE_LEVEL1_ORG_UNITS = [
    {"id": "ROOT_OU", "name": "Lao PDR"},
]

SAMPLE_METADATA_RESPONSE = {
    "status": "OK",
    "stats": {"created": 0, "updated": 2, "deleted": 0, "ignored": 0, "total": 2},
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


def test_data_element_defaults() -> None:
    de = Dhis2DataElement(id="abc12345678", name="Test", shortName="T")
    assert de.domainType == "AGGREGATE"
    assert de.valueType == "NUMBER"
    assert de.aggregationType == "SUM"


def test_data_set_period_type() -> None:
    ds = Dhis2DataSet(id="abc12345678", name="Test", shortName="T", periodType="Yearly")
    assert ds.periodType == "Yearly"


# ---------------------------------------------------------------------------
# Task tests
# ---------------------------------------------------------------------------


def test_ensure_dhis2_metadata() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_LEVEL1_ORG_UNITS
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE

    result = ensure_dhis2_metadata.fn(mock_client)

    mock_client.fetch_metadata.assert_called_once_with("organisationUnits", fields="id,name", filters=["level:eq:1"])
    mock_client.post_metadata.assert_called_once()
    payload = mock_client.post_metadata.call_args[0][0]
    assert len(payload["dataElements"]) == 1
    assert payload["dataElements"][0]["name"] == "Prefect - Population"
    assert len(payload["dataSets"]) == 1
    assert payload["dataSets"][0]["periodType"] == "Yearly"
    assert payload["dataSets"][0]["organisationUnits"] == [{"id": "ROOT_OU"}]
    assert result["status"] == "OK"


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

    results = fetch_population_data.fn(["LAO"], 2020, 2023)

    assert len(results) == 3  # one null value filtered out
    assert all(isinstance(r, CountryPopulation) for r in results)
    assert results[0].iso3 == "LAO"
    assert results[0].population == 7633779


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

    mapping = resolve_org_units.fn(mock_client, ["LAO"])

    assert len(mapping) == 1
    assert "LAO" in mapping
    assert mapping["LAO"].org_unit_uid == "OU_LAO"
    assert mapping["LAO"].org_unit_name == "Lao PDR"


def test_resolve_org_units_missing() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_organisation_units_by_code.return_value = SAMPLE_ORG_UNITS

    mapping = resolve_org_units.fn(mock_client, ["LAO", "ZZZ"])
    assert len(mapping) == 1  # ZZZ not found


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
    assert report.dhis2_url == "https://dhis2.example.org"
    assert report.record_count == 2
    assert report.org_units_resolved == 1
    assert "https://dhis2.example.org" in report.markdown
    assert "Lao PDR" in report.markdown
    assert "7,633,779" in report.markdown
    assert "OU_LAO" in report.markdown


def test_build_report_empty() -> None:
    query = PopulationQuery(iso3_codes=["ZZZ"], start_year=2023, end_year=2023)

    report = build_report.fn("https://dhis2.example.org", query, [], {})

    assert report.record_count == 0
    assert "No population data" in report.markdown
    assert "No matching org units" in report.markdown


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("dhis2_worldbank_import.httpx.Client")
@patch.object(Dhis2Credentials, "get_client")
def test_flow_runs(mock_get_client: MagicMock, mock_httpx_cls: MagicMock) -> None:
    # Mock DHIS2 client
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_LEVEL1_ORG_UNITS
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE
    mock_client.fetch_organisation_units_by_code.return_value = SAMPLE_ORG_UNITS
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

    state = dhis2_worldbank_import_flow(return_state=True)
    assert state.is_completed()
    report = state.result()
    assert isinstance(report, PopulationReport)
    assert report.record_count == 3
    assert report.org_units_resolved == 1
