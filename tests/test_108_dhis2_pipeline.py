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


def _mock_response(json_data: dict, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


def _side_effect(url: str, **kwargs: object) -> MagicMock:
    if "system/info" in url:
        return _mock_response(SAMPLE_SYSTEM_INFO)
    if "organisationUnits" in url:
        return _mock_response({"organisationUnits": SAMPLE_ORG_UNITS})
    if "dataElements" in url:
        return _mock_response({"dataElements": SAMPLE_DATA_ELEMENTS})
    if "indicators" in url:
        return _mock_response({"indicators": SAMPLE_INDICATORS})
    return _mock_response({})


@patch("httpx.get", side_effect=_side_effect)
def test_connect_and_verify(mock_get: MagicMock) -> None:
    conn = Dhis2Connection()
    response = connect_and_verify.fn(conn, "district")
    assert isinstance(response, Dhis2ApiResponse)
    assert response.status_code == 200
    assert response.endpoint == "system/info"


@patch("httpx.get", side_effect=_side_effect)
def test_fetch_all_metadata(mock_get: MagicMock) -> None:
    conn = Dhis2Connection()
    metadata = fetch_all_metadata.fn(conn, "district")
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


@patch("httpx.get", side_effect=_side_effect)
def test_pipeline_stages(mock_get: MagicMock) -> None:
    state = dhis2_pipeline_flow(return_state=True)
    assert state.is_completed()
    result = state.result()
    assert len(result.stages) == 3
    assert all(s.status == "completed" for s in result.stages)


@patch("httpx.get", side_effect=_side_effect)
def test_flow_runs(mock_get: MagicMock) -> None:
    state = dhis2_pipeline_flow(return_state=True)
    assert state.is_completed()
