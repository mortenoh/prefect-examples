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

SAMPLE_WORLDPOP_TASK_RESPONSE = {
    "status": "finished",
    "data": {
        "agesexpyramid": [
            {"class": "0", "male": 500.0, "female": 490.0},
            {"class": "1", "male": 480.0, "female": 470.0},
            {"class": "2", "male": 450.0, "female": 440.0},
            {"class": "3", "male": 420.0, "female": 410.0},
            {"class": "4", "male": 400.0, "female": 390.0},
            {"class": "5", "male": 380.0, "female": 370.0},
            {"class": "6", "male": 350.0, "female": 340.0},
            {"class": "7", "male": 320.0, "female": 310.0},
            {"class": "8", "male": 300.0, "female": 290.0},
            {"class": "9", "male": 280.0, "female": 270.0},
            {"class": "10", "male": 250.0, "female": 240.0},
            {"class": "11", "male": 220.0, "female": 210.0},
            {"class": "12", "male": 180.0, "female": 170.0},
            {"class": "13", "male": 140.0, "female": 130.0},
            {"class": "14", "male": 100.0, "female": 90.0},
            {"class": "15", "male": 60.0, "female": 50.0},
            {"class": "16", "male": 30.0, "female": 20.0},
        ],
    },
}


def test_ensure_dhis2_metadata() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.side_effect = [
        SAMPLE_OU_WITH_GEOM,  # level-2 org units
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
    mock_resp_created = MagicMock()
    mock_resp_created.json.return_value = {"status": "created", "taskid": "abc123"}
    mock_resp_created.raise_for_status = MagicMock()

    mock_resp_finished = MagicMock()
    mock_resp_finished.json.return_value = SAMPLE_WORLDPOP_TASK_RESPONSE
    mock_resp_finished.raise_for_status = MagicMock()

    mock_http_initial = MagicMock()
    mock_http_initial.post.return_value = mock_resp_created
    mock_http_initial.__enter__ = MagicMock(return_value=mock_http_initial)
    mock_http_initial.__exit__ = MagicMock(return_value=False)

    mock_http_poll = MagicMock()
    mock_http_poll.get.return_value = mock_resp_finished
    mock_http_poll.__enter__ = MagicMock(return_value=mock_http_poll)
    mock_http_poll.__exit__ = MagicMock(return_value=False)

    mock_client_cls.side_effect = [mock_http_initial, mock_http_poll]

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
        SAMPLE_OU_WITH_GEOM,  # level-2 org units
        SAMPLE_COCS,
    ]
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE
    mock_get_client.return_value = mock_client

    mock_resp_created = MagicMock()
    mock_resp_created.json.return_value = {"status": "created", "taskid": "flow123"}
    mock_resp_created.raise_for_status = MagicMock()

    mock_resp_finished = MagicMock()
    mock_resp_finished.json.return_value = SAMPLE_WORLDPOP_TASK_RESPONSE
    mock_resp_finished.raise_for_status = MagicMock()

    mock_http_initial = MagicMock()
    mock_http_initial.post.return_value = mock_resp_created
    mock_http_initial.__enter__ = MagicMock(return_value=mock_http_initial)
    mock_http_initial.__exit__ = MagicMock(return_value=False)

    mock_http_poll = MagicMock()
    mock_http_poll.get.return_value = mock_resp_finished
    mock_http_poll.__enter__ = MagicMock(return_value=mock_http_poll)
    mock_http_poll.__exit__ = MagicMock(return_value=False)

    mock_httpx_cls.side_effect = [mock_http_initial, mock_http_poll]

    state = dhis2_worldpop_population_import_flow(return_state=True)
    assert state.is_completed()
    result = state.result()
    assert isinstance(result, ImportResult)
    assert result.imported == 2
    assert result.org_units[0].id == "ROOT_OU"
