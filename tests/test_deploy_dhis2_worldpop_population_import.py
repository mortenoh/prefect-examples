"""Tests for the dhis2_worldpop_population_import deployment flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "deploy_dhis2_worldpop_population_import",
    Path(__file__).resolve().parent.parent / "deployments" / "dhis2_worldpop_population_import" / "flow.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["deploy_dhis2_worldpop_population_import"] = _mod
_spec.loader.exec_module(_mod)

from prefect_dhis2 import Dhis2Client  # noqa: E402
from prefect_dhis2.credentials import Dhis2Credentials  # noqa: E402

ImportQuery = _mod.ImportQuery
OrgUnitGeo = _mod.OrgUnitGeo
WorldPopResult = _mod.WorldPopResult
CocMapping = _mod.CocMapping
DataValue = _mod.DataValue
ImportResult = _mod.ImportResult
ensure_dhis2_metadata = _mod.ensure_dhis2_metadata
fetch_worldpop_population = _mod.fetch_worldpop_population
build_data_values = _mod.build_data_values
import_to_dhis2 = _mod.import_to_dhis2
dhis2_worldpop_population_import_flow = _mod.dhis2_worldpop_population_import_flow

SAMPLE_OU_WITH_GEOM = [
    {
        "id": "ROOT_OU",
        "name": "Root",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[100.0, 14.0], [108.0, 14.0], [108.0, 23.0], [100.0, 23.0], [100.0, 14.0]]],
        },
    }
]

SAMPLE_COCS = [
    {"id": "COC_MALE_1", "name": "Male", "categoryOptions": [{"id": "PfWpSexMal1"}]},
    {"id": "COC_FEML_1", "name": "Female", "categoryOptions": [{"id": "PfWpSexFem1"}]},
]

SAMPLE_METADATA_RESPONSE = {
    "status": "OK",
    "stats": {"created": 6, "updated": 0, "deleted": 0, "ignored": 0, "total": 6},
}

SAMPLE_IMPORT_RESPONSE = {
    "status": "SUCCESS",
    "importCount": {"imported": 2, "updated": 0, "ignored": 0, "deleted": 0},
}

SAMPLE_WORLDPOP_RESPONSE = {
    "status": "finished",
    "data": {
        "agesexpyramid": {
            "M_0": 500.0,
            "M_1": 480.0,
            "M_2": 450.0,
            "M_3": 420.0,
            "M_4": 400.0,
            "M_5": 380.0,
            "M_6": 350.0,
            "M_7": 320.0,
            "M_8": 300.0,
            "M_9": 280.0,
            "M_10": 250.0,
            "M_11": 220.0,
            "M_12": 180.0,
            "M_13": 140.0,
            "M_14": 100.0,
            "M_15": 60.0,
            "M_16": 30.0,
            "F_0": 490.0,
            "F_1": 470.0,
            "F_2": 440.0,
            "F_3": 410.0,
            "F_4": 390.0,
            "F_5": 370.0,
            "F_6": 340.0,
            "F_7": 310.0,
            "F_8": 290.0,
            "F_9": 270.0,
            "F_10": 240.0,
            "F_11": 210.0,
            "F_12": 170.0,
            "F_13": 130.0,
            "F_14": 90.0,
            "F_15": 50.0,
            "F_16": 20.0,
        }
    },
}


def test_ensure_dhis2_metadata() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.side_effect = [
        SAMPLE_OU_WITH_GEOM,
        [],  # no existing category options
        SAMPLE_COCS,
    ]
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE

    org_units, coc_mapping = ensure_dhis2_metadata.fn(mock_client)

    assert org_units[0].id == "ROOT_OU"
    assert coc_mapping.male == "COC_MALE_1"
    assert coc_mapping.female == "COC_FEML_1"
    payload = mock_client.post_metadata.call_args[0][0]
    assert "categoryOptions" in payload
    assert "categoryCombos" in payload
    assert payload["dataSets"][0]["organisationUnits"] == [{"id": "ROOT_OU"}]


@patch("deploy_dhis2_worldpop_population_import.httpx.Client")
def test_fetch_worldpop_population(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = SAMPLE_WORLDPOP_RESPONSE
    mock_resp.raise_for_status = MagicMock()
    mock_client_cls.return_value.__enter__ = MagicMock(return_value=MagicMock())
    mock_client_cls.return_value.__enter__.return_value.get.return_value = mock_resp
    mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

    ou = OrgUnitGeo(id="ROOT_OU", name="Root", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])
    result = fetch_worldpop_population.fn(ou, 2020)

    assert result.male > 0
    assert result.female > 0


def test_build_data_values() -> None:
    results = [WorldPopResult(org_unit_id="ROOT_OU", org_unit_name="Root", male=4860.0, female=4690.0)]
    coc_mapping = CocMapping(male="COC_MALE_1", female="COC_FEML_1")

    data_values = build_data_values.fn(results, 2020, coc_mapping)

    assert len(data_values) == 2
    assert data_values[0].dataElement == "PfWpPopEst1"
    assert data_values[0].orgUnit == "ROOT_OU"
    assert data_values[0].period == "2020"


def test_import_to_dhis2() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE

    org_units = [OrgUnitGeo(id="ROOT_OU", name="Root", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])]
    data_values = [
        DataValue(
            dataElement="PfWpPopEst1",
            period="2020",
            orgUnit="ROOT_OU",
            categoryOptionCombo="COC_MALE_1",
            value="4860",
        ),
        DataValue(
            dataElement="PfWpPopEst1",
            period="2020",
            orgUnit="ROOT_OU",
            categoryOptionCombo="COC_FEML_1",
            value="4690",
        ),
    ]

    result = import_to_dhis2.fn(mock_client, "https://dhis2.example.org", org_units, data_values)

    assert isinstance(result, ImportResult)
    assert result.imported == 2
    assert result.total == 2


@patch("deploy_dhis2_worldpop_population_import.httpx.Client")
@patch.object(Dhis2Credentials, "get_client")
def test_flow_runs(mock_get_client: MagicMock, mock_httpx_cls: MagicMock) -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.side_effect = [
        SAMPLE_OU_WITH_GEOM,
        [],  # no existing category options
        SAMPLE_COCS,
    ]
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE
    mock_get_client.return_value = mock_client

    mock_resp = MagicMock()
    mock_resp.json.return_value = SAMPLE_WORLDPOP_RESPONSE
    mock_resp.raise_for_status = MagicMock()

    mock_http = MagicMock()
    mock_http.get.return_value = mock_resp
    mock_http.__enter__ = MagicMock(return_value=mock_http)
    mock_http.__exit__ = MagicMock(return_value=False)
    mock_httpx_cls.return_value = mock_http

    state = dhis2_worldpop_population_import_flow(return_state=True)
    assert state.is_completed()
    result = state.result()
    assert isinstance(result, ImportResult)
    assert result.imported == 2
    assert result.org_units[0].id == "ROOT_OU"
