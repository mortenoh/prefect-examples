"""Tests for flow 108 -- DHIS2 Full Pipeline."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "flow_108",
    Path(__file__).resolve().parent.parent / "flows" / "108_dhis2_pipeline.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_108"] = _mod
_spec.loader.exec_module(_mod)

Dhis2Connection = _mod.Dhis2Connection
Dhis2ApiResponse = _mod.Dhis2ApiResponse
PipelineStage = _mod.PipelineStage
QualityResult = _mod.QualityResult
Dhis2PipelineResult = _mod.Dhis2PipelineResult
connect_and_verify = _mod.connect_and_verify
fetch_all_metadata = _mod.fetch_all_metadata
validate_metadata = _mod.validate_metadata
build_dashboard = _mod.build_dashboard
dhis2_pipeline_flow = _mod.dhis2_pipeline_flow

SAMPLE_SYSTEM_INFO = {"version": "2.43-SNAPSHOT", "revision": "abc123"}
SAMPLE_ORG_UNITS = [
    {"id": "OU1", "name": "National", "level": 1, "parent": None},
    {"id": "OU2", "name": "Region", "level": 2, "parent": {"id": "OU1"}},
]
SAMPLE_DATA_ELEMENTS = [
    {"id": "DE1", "name": "ANC 1st", "valueType": "NUMBER"},
    {"id": "DE2", "name": "Malaria", "valueType": "INTEGER"},
]
SAMPLE_INDICATORS = [
    {"id": "IND1", "name": "ANC Rate", "numerator": "#{a.b}", "denominator": "1"},
]


def _mock_client_with_side_effect() -> MagicMock:
    """Create a mock client that dispatches based on the URL path."""

    def _get_side_effect(url: str, **kwargs: object) -> MagicMock:
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        if "system/info" in url:
            resp.json.return_value = SAMPLE_SYSTEM_INFO
        elif "organisationUnits" in url:
            resp.json.return_value = {"organisationUnits": SAMPLE_ORG_UNITS}
        elif "dataElements" in url:
            resp.json.return_value = {"dataElements": SAMPLE_DATA_ELEMENTS}
        elif "indicators" in url:
            resp.json.return_value = {"indicators": SAMPLE_INDICATORS}
        else:
            resp.json.return_value = {}
        return resp

    mock_cl = MagicMock()
    mock_cl.__enter__ = MagicMock(return_value=mock_cl)
    mock_cl.__exit__ = MagicMock(return_value=False)
    mock_cl.get.side_effect = _get_side_effect
    return mock_cl


@patch.object(Dhis2Connection, "get_client")
def test_connect_and_verify(mock_get_client: MagicMock) -> None:
    mock_get_client.return_value = _mock_client_with_side_effect()
    conn = Dhis2Connection()
    response = connect_and_verify.fn(conn)
    assert isinstance(response, Dhis2ApiResponse)
    assert response.endpoint == "system/info"


@patch.object(Dhis2Connection, "get_client")
def test_fetch_all_metadata(mock_get_client: MagicMock) -> None:
    mock_get_client.return_value = _mock_client_with_side_effect()
    conn = Dhis2Connection()
    metadata = fetch_all_metadata.fn(conn)
    assert "organisationUnits" in metadata
    assert "dataElements" in metadata
    assert "indicators" in metadata
    assert len(metadata["organisationUnits"]) == 2
    assert len(metadata["dataElements"]) == 2
    assert len(metadata["indicators"]) == 1


def test_validate_metadata_all_pass() -> None:
    metadata = {
        "organisationUnits": SAMPLE_ORG_UNITS,
        "dataElements": SAMPLE_DATA_ELEMENTS,
        "indicators": SAMPLE_INDICATORS,
    }
    quality = validate_metadata.fn(metadata)
    assert isinstance(quality, QualityResult)
    assert quality.checks_passed == quality.checks_total
    assert quality.score == 1.0
    assert quality.issues == []


def test_validate_metadata_with_issues() -> None:
    metadata = {
        "organisationUnits": [{"id": "OU1", "level": 0}],
        "dataElements": [{"id": "DE1"}],
        "indicators": [{"id": "IND1"}],
    }
    quality = validate_metadata.fn(metadata)
    assert quality.score < 1.0
    assert len(quality.issues) > 0


def test_build_dashboard() -> None:
    result = Dhis2PipelineResult(
        stages=[
            PipelineStage(name="connect", status="completed", record_count=1, duration=0.01),
            PipelineStage(name="fetch", status="completed", record_count=5, duration=0.05),
        ],
        total_records=5,
        quality_score=1.0,
        duration=0.1,
    )
    md = build_dashboard.fn(result)
    assert "DHIS2 Pipeline Dashboard" in md
    assert "5" in md
    assert "connect" in md
    assert "fetch" in md


@patch.object(Dhis2Connection, "get_client")
def test_pipeline_stages(mock_get_client: MagicMock) -> None:
    mock_get_client.return_value = _mock_client_with_side_effect()
    state = dhis2_pipeline_flow(return_state=True)
    assert state.is_completed()
    result = state.result()
    assert len(result.stages) == 3
    assert all(s.status == "completed" for s in result.stages)


@patch.object(Dhis2Connection, "get_client")
def test_flow_runs(mock_get_client: MagicMock) -> None:
    mock_get_client.return_value = _mock_client_with_side_effect()
    state = dhis2_pipeline_flow(return_state=True)
    assert state.is_completed()
