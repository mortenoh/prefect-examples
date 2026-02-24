"""Tests for flow 108 -- DHIS2 Full Pipeline."""

import importlib.util
import sys
from pathlib import Path

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


def test_connect_and_verify() -> None:
    conn = Dhis2Connection()
    response = connect_and_verify.fn(conn, "district")
    assert isinstance(response, Dhis2ApiResponse)
    assert response.status_code == 200


def test_fetch_all_metadata() -> None:
    conn = Dhis2Connection()
    metadata = fetch_all_metadata.fn(conn, "district")
    assert "organisationUnits" in metadata
    assert "dataElements" in metadata
    assert "indicators" in metadata
    assert len(metadata["organisationUnits"]) == 20
    assert len(metadata["dataElements"]) == 15
    assert len(metadata["indicators"]) == 10


def test_validate_metadata() -> None:
    conn = Dhis2Connection()
    metadata = fetch_all_metadata.fn(conn, "district")
    quality = validate_metadata.fn(metadata)
    assert isinstance(quality, QualityResult)
    assert quality.score > 0.0
    assert quality.checks_total > 0


def test_validate_all_pass() -> None:
    conn = Dhis2Connection()
    metadata = fetch_all_metadata.fn(conn, "district")
    quality = validate_metadata.fn(metadata)
    # All simulated data should pass validation
    assert quality.checks_passed == quality.checks_total
    assert quality.score == 1.0


def test_build_dashboard() -> None:
    result = Dhis2PipelineResult(
        stages=[
            PipelineStage(name="connect", status="completed", record_count=1, duration=0.01),
            PipelineStage(name="fetch", status="completed", record_count=45, duration=0.05),
        ],
        total_records=45,
        quality_score=1.0,
        duration=0.1,
    )
    md = build_dashboard.fn(result)
    assert "DHIS2 Pipeline Dashboard" in md
    assert "45" in md
    assert "connect" in md
    assert "fetch" in md


def test_pipeline_stages() -> None:
    state = dhis2_pipeline_flow(return_state=True)
    assert state.is_completed()
    result = state.result()
    assert len(result.stages) == 3
    assert all(s.status == "completed" for s in result.stages)


def test_flow_runs() -> None:
    state = dhis2_pipeline_flow(return_state=True)
    assert state.is_completed()
