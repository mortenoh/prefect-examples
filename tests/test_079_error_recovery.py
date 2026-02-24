"""Tests for flow 079 -- Error Recovery."""

import importlib.util
import sys
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "flow_079",
    Path(__file__).resolve().parent.parent / "flows" / "079_error_recovery.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_079"] = _mod
_spec.loader.exec_module(_mod)

Checkpoint = _mod.Checkpoint
CheckpointStore = _mod.CheckpointStore
RecoveryReport = _mod.RecoveryReport
load_checkpoints = _mod.load_checkpoints
save_checkpoint = _mod.save_checkpoint
should_run_stage = _mod.should_run_stage
execute_stage = _mod.execute_stage
run_with_checkpoints = _mod.run_with_checkpoints
error_recovery_flow = _mod.error_recovery_flow


def test_checkpoint_store_empty() -> None:
    store = CheckpointStore(pipeline_id="test")
    assert len(store.checkpoints) == 0


def test_load_checkpoints_missing(tmp_path: Path) -> None:
    store = load_checkpoints.fn(tmp_path / "missing.json")
    assert len(store.checkpoints) == 0


def test_save_and_load_checkpoint(tmp_path: Path) -> None:
    path = tmp_path / "cp.json"
    store = CheckpointStore(pipeline_id="test")
    store = save_checkpoint.fn(store, "extract", "completed", {"count": 5}, path)
    loaded = load_checkpoints.fn(path)
    assert "extract" in loaded.checkpoints
    assert loaded.checkpoints["extract"].status == "completed"


def test_should_run_completed_stage() -> None:
    store = CheckpointStore(pipeline_id="test")
    store.checkpoints["extract"] = Checkpoint(
        stage="extract",
        status="completed",
        data={},
        timestamp="2025-01-01T00:00:00",
    )
    assert should_run_stage.fn(store, "extract") is False
    assert should_run_stage.fn(store, "validate") is True


def test_execute_stage_extract() -> None:
    result = execute_stage.fn("extract", {})
    assert len(result["records"]) == 5


def test_execute_stage_failure() -> None:
    with pytest.raises(RuntimeError, match="Simulated failure"):
        execute_stage.fn("transform", {}, fail_on="transform")


def test_full_pipeline_success(tmp_path: Path) -> None:
    report = run_with_checkpoints.fn(
        ["extract", "validate", "transform", "load"],
        tmp_path / "cp.json",
        fail_on=None,
    )
    assert report.final_status == "completed"
    assert report.stages_executed == 4


def test_recovery_after_failure(tmp_path: Path) -> None:
    """Fail at transform, then re-run and verify recovery."""
    cp_path = tmp_path / "cp.json"

    # First run: fail at transform
    report1 = run_with_checkpoints.fn(
        ["extract", "validate", "transform", "load"],
        cp_path,
        fail_on="transform",
    )
    assert report1.final_status == "failed"
    assert report1.stages_executed == 2  # extract, validate

    # Second run: no failure, should recover
    report2 = run_with_checkpoints.fn(
        ["extract", "validate", "transform", "load"],
        cp_path,
        fail_on=None,
    )
    assert report2.final_status == "completed"
    assert report2.stages_recovered == 2  # extract, validate skipped
    assert report2.stages_executed == 2  # transform, load executed


def test_flow_runs(tmp_path: Path) -> None:
    state = error_recovery_flow(work_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
