"""Tests for DHIS2 WorldPop Population Import flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_spec = importlib.util.spec_from_file_location(
    "dhis2_worldpop_population_import",
    Path(__file__).resolve().parent.parent.parent / "flows" / "dhis2" / "dhis2_worldpop_population_import.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["dhis2_worldpop_population_import"] = _mod
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

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

SAMPLE_OU_WITH_GEOM = [
    {
        "id": "ROOT_OU",
        "name": "Lao PDR",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[100.0, 14.0], [108.0, 14.0], [108.0, 23.0], [100.0, 23.0], [100.0, 14.0]]],
        },
    }
]

SAMPLE_OU_POINT = [
    {
        "id": "POINT_OU",
        "name": "Point Org",
        "geometry": {"type": "Point", "coordinates": [100.0, 14.0]},
    }
]

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

# Task endpoint returns list-of-objects format (used by async polling via /v1/tasks/)
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

SAMPLE_WORLDPOP_ERROR_RESPONSE = {
    "status": "finished",
    "error": True,
    "error_message": "No User Description for this type of error: IndexError",
}

SAMPLE_WORLDPOP_POP_RESPONSE = {
    "status": "finished",
    "error": False,
    "data": {"total_population": 9550.0},
}

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

# 500+480+450+420+400+380+350+320+300+280+250+220+180+140+100+60+30
EXPECTED_MALE_TOTAL = 4860.0
# 490+470+440+410+390+370+340+310+290+270+240+210+170+130+90+50+20
EXPECTED_FEMALE_TOTAL = 4690.0


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_import_query_defaults() -> None:
    q = ImportQuery()
    assert q.years == list(range(2015, 2025))


# ---------------------------------------------------------------------------
# Task tests
# ---------------------------------------------------------------------------


def test_ensure_dhis2_metadata() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.side_effect = [
        SAMPLE_OU_WITH_GEOM,  # level-2 org units
        SAMPLE_COCS,  # categoryOptionCombos
    ]
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE

    org_units, coc_mapping = ensure_dhis2_metadata.fn(mock_client)

    assert len(org_units) == 1
    assert org_units[0].id == "ROOT_OU"
    assert org_units[0].name == "Lao PDR"
    assert coc_mapping.male == "COC_MALE_1"
    assert coc_mapping.female == "COC_FEML_1"

    mock_client.post_metadata.assert_called_once()
    payload = mock_client.post_metadata.call_args[0][0]
    assert "categoryOptions" in payload
    assert "categories" in payload
    assert "categoryCombos" in payload
    assert "dataElements" in payload
    assert "dataSets" in payload
    assert len(payload["categoryOptions"]) == 2
    assert payload["categoryOptions"][0]["name"] == "PR: Male"
    assert payload["categoryOptions"][1]["name"] == "PR: Female"
    assert len(payload["categories"]) == 1
    assert len(payload["categoryCombos"]) == 1
    assert len(payload["dataElements"]) == 1
    assert len(payload["dataSets"]) == 1
    assert payload["dataSets"][0]["organisationUnits"] == [{"id": "ROOT_OU"}]
    mock_client.run_maintenance.assert_called_once_with("categoryOptionComboUpdate")


def test_ensure_dhis2_metadata_no_polygon() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_OU_POINT

    with pytest.raises(ValueError, match="No level-2 org units with Polygon"):
        ensure_dhis2_metadata.fn(mock_client)


@patch("dhis2_worldpop_population_import.httpx.Client")
def test_fetch_worldpop_population(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = SAMPLE_WORLDPOP_RESPONSE
    mock_resp.raise_for_status = MagicMock()

    mock_http = MagicMock()
    mock_http.post.return_value = mock_resp
    mock_http.__enter__ = MagicMock(return_value=mock_http)
    mock_http.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_http

    ou = OrgUnitGeo(
        id="ROOT_OU",
        name="Lao PDR",
        geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"],
    )
    result = fetch_worldpop_population.fn(ou, 2020)

    assert isinstance(result, WorldPopResult)
    assert result.org_unit_id == "ROOT_OU"
    assert result.org_unit_name == "Lao PDR"
    assert result.male == EXPECTED_MALE_TOTAL
    assert result.female == EXPECTED_FEMALE_TOTAL


@patch("dhis2_worldpop_population_import.httpx.Client")
def test_fetch_worldpop_population_async(mock_client_cls: MagicMock) -> None:
    async_created = {"status": "created", "taskid": "abc123"}

    mock_resp_created = MagicMock()
    mock_resp_created.json.return_value = async_created
    mock_resp_created.raise_for_status = MagicMock()

    mock_resp_finished = MagicMock()
    mock_resp_finished.json.return_value = SAMPLE_WORLDPOP_TASK_RESPONSE
    mock_resp_finished.raise_for_status = MagicMock()

    # First Client context: initial POST returns "created"
    # Second Client context (polling): GET /v1/tasks/{id} returns "finished"
    mock_http_initial = MagicMock()
    mock_http_initial.post.return_value = mock_resp_created
    mock_http_initial.__enter__ = MagicMock(return_value=mock_http_initial)
    mock_http_initial.__exit__ = MagicMock(return_value=False)

    mock_http_poll = MagicMock()
    mock_http_poll.get.return_value = mock_resp_finished
    mock_http_poll.__enter__ = MagicMock(return_value=mock_http_poll)
    mock_http_poll.__exit__ = MagicMock(return_value=False)

    mock_client_cls.side_effect = [mock_http_initial, mock_http_poll]

    ou = OrgUnitGeo(
        id="ROOT_OU",
        name="Lao PDR",
        geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"],
    )
    result = fetch_worldpop_population.fn(ou, 2020)

    assert result.male == EXPECTED_MALE_TOTAL
    assert result.female == EXPECTED_FEMALE_TOTAL


@patch("dhis2_worldpop_population_import.httpx.Client")
def test_fetch_worldpop_population_fallback(mock_client_cls: MagicMock) -> None:
    """When wpgpas errors (e.g. no coverage), fall back to wpgppop with 50/50 split."""

    def _make_client(resp: MagicMock) -> MagicMock:
        m = MagicMock()
        m.post.return_value = resp
        m.get.return_value = resp
        m.__enter__ = MagicMock(return_value=m)
        m.__exit__ = MagicMock(return_value=False)
        return m

    # wpgpas: POST returns "created", poll GET returns error
    resp_created_1 = MagicMock()
    resp_created_1.json.return_value = {"status": "created", "taskid": "t1"}
    resp_created_1.raise_for_status = MagicMock()

    resp_error = MagicMock()
    resp_error.json.return_value = SAMPLE_WORLDPOP_ERROR_RESPONSE
    resp_error.raise_for_status = MagicMock()

    # wpgppop: POST returns "created", poll GET returns total_population
    resp_created_2 = MagicMock()
    resp_created_2.json.return_value = {"status": "created", "taskid": "t2"}
    resp_created_2.raise_for_status = MagicMock()

    resp_pop = MagicMock()
    resp_pop.json.return_value = SAMPLE_WORLDPOP_POP_RESPONSE
    resp_pop.raise_for_status = MagicMock()

    mock_client_cls.side_effect = [
        _make_client(resp_created_1),  # wpgpas POST
        _make_client(resp_error),  # wpgpas poll GET -> error
        _make_client(resp_created_2),  # wpgppop POST
        _make_client(resp_pop),  # wpgppop poll GET -> success
    ]

    ou = OrgUnitGeo(
        id="ROOT_OU",
        name="Lao PDR",
        geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"],
    )
    result = fetch_worldpop_population.fn(ou, 2020)

    assert result.male == 4775.0  # 9550 / 2
    assert result.female == 4775.0


def test_build_data_values() -> None:
    results = [
        WorldPopResult(org_unit_id="ROOT_OU", org_unit_name="Lao PDR", male=4860.0, female=4690.0),
    ]
    coc_mapping = CocMapping(male="COC_MALE_1", female="COC_FEML_1")

    data_values = build_data_values.fn(results, 2020, coc_mapping)

    assert len(data_values) == 2
    male_dv = [dv for dv in data_values if dv.categoryOptionCombo == "COC_MALE_1"]
    female_dv = [dv for dv in data_values if dv.categoryOptionCombo == "COC_FEML_1"]
    assert len(male_dv) == 1
    assert len(female_dv) == 1
    assert male_dv[0].dataElement == "PfWpPopEst1"
    assert male_dv[0].orgUnit == "ROOT_OU"
    assert male_dv[0].period == "2020"
    assert male_dv[0].value == "4860"
    assert female_dv[0].value == "4690"


def test_import_to_dhis2() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE

    org_units = [OrgUnitGeo(id="ROOT_OU", name="Lao PDR", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])]
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
    assert "WorldPop" in result.markdown
    mock_client.post_data_values.assert_called_once()
    payload = mock_client.post_data_values.call_args[0][0]
    assert len(payload["dataValues"]) == 2


def test_import_to_dhis2_empty() -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    org_units = [OrgUnitGeo(id="ROOT_OU", name="Lao PDR", geometry=SAMPLE_OU_WITH_GEOM[0]["geometry"])]

    result = import_to_dhis2.fn(mock_client, "https://dhis2.example.org", org_units, [])

    assert result.total == 0
    assert "No data values" in result.markdown
    mock_client.post_data_values.assert_not_called()


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("dhis2_worldpop_population_import._query_worldpop_polygon")
@patch.object(Dhis2Credentials, "get_client")
def test_flow_runs(mock_get_client: MagicMock, mock_query: MagicMock) -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.side_effect = [
        SAMPLE_OU_WITH_GEOM,
        SAMPLE_COCS,
    ]
    mock_client.post_metadata.return_value = SAMPLE_METADATA_RESPONSE
    mock_client.post_data_values.return_value = SAMPLE_IMPORT_RESPONSE
    mock_get_client.return_value = mock_client

    # Mock the WorldPop query to return population totals directly
    mock_query.return_value = (EXPECTED_MALE_TOTAL, EXPECTED_FEMALE_TOTAL)

    state = dhis2_worldpop_population_import_flow(query=ImportQuery(years=[2020]), return_state=True)
    assert state.is_completed()
    result = state.result()
    assert isinstance(result, ImportResult)
    assert result.imported == 2
    assert len(result.org_units) == 1
    assert result.org_units[0].id == "ROOT_OU"
