"""Tests for flow 099 -- Multi-Pipeline Orchestrator."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_099",
    Path(__file__).resolve().parent.parent / "flows" / "099_multi_pipeline_orchestrator.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_099"] = _mod
_spec.loader.exec_module(_mod)

PipelineStatus = _mod.PipelineStatus
PipelineDataRecord = _mod.PipelineDataRecord
OrchestratorResult = _mod.OrchestratorResult
generate_pipeline_data = _mod.generate_pipeline_data
run_ingest_pipeline = _mod.run_ingest_pipeline
run_transform_pipeline = _mod.run_transform_pipeline
run_export_pipeline = _mod.run_export_pipeline
run_quality_pipeline = _mod.run_quality_pipeline
aggregate_status = _mod.aggregate_status
multi_pipeline_orchestrator_flow = _mod.multi_pipeline_orchestrator_flow


def test_generate_pipeline_data() -> None:
    data = generate_pipeline_data.fn()
    assert len(data) == 50
    assert all(isinstance(d, PipelineDataRecord) for d in data)


def test_run_ingest_pipeline() -> None:
    data = generate_pipeline_data.fn()
    status = run_ingest_pipeline.fn(data)
    assert isinstance(status, PipelineStatus)
    assert status.status == "success"
    assert status.records_processed == 50


def test_run_transform_pipeline() -> None:
    data = generate_pipeline_data.fn()
    status = run_transform_pipeline.fn(data)
    assert status.status == "success"


def test_aggregate_all_success() -> None:
    statuses = [
        PipelineStatus(pipeline_name="a", status="success", records_processed=10, duration_seconds=0.1, error=""),
        PipelineStatus(pipeline_name="b", status="success", records_processed=20, duration_seconds=0.2, error=""),
    ]
    result = aggregate_status.fn(statuses)
    assert result.overall_status == "healthy"
    assert result.successful == 2
    assert result.failed == 0


def test_aggregate_degraded() -> None:
    statuses = [
        PipelineStatus(pipeline_name="a", status="success", records_processed=10, duration_seconds=0.1, error=""),
        PipelineStatus(pipeline_name="b", status="success", records_processed=20, duration_seconds=0.2, error=""),
        PipelineStatus(pipeline_name="c", status="failed", records_processed=0, duration_seconds=0.1, error="err"),
    ]
    result = aggregate_status.fn(statuses)
    assert result.overall_status == "degraded"


def test_aggregate_critical() -> None:
    statuses = [
        PipelineStatus(pipeline_name="a", status="failed", records_processed=0, duration_seconds=0.1, error="e1"),
        PipelineStatus(pipeline_name="b", status="failed", records_processed=0, duration_seconds=0.1, error="e2"),
        PipelineStatus(pipeline_name="c", status="success", records_processed=5, duration_seconds=0.1, error=""),
    ]
    result = aggregate_status.fn(statuses)
    assert result.overall_status == "critical"


def test_flow_runs() -> None:
    state = multi_pipeline_orchestrator_flow(return_state=True)
    assert state.is_completed()
