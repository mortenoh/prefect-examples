"""Tests for flow 028 — Result Persistence."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "core_result_persistence",
    Path(__file__).resolve().parent.parent.parent / "flows" / "core" / "core_result_persistence.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["core_result_persistence"] = _mod
_spec.loader.exec_module(_mod)

compute_metrics = _mod.compute_metrics
build_summary = _mod.build_summary
transient_task = _mod.transient_task
result_persistence_flow = _mod.result_persistence_flow


def test_compute_metrics() -> None:
    result = compute_metrics.fn([10, 20, 30])
    assert isinstance(result, dict)
    assert result["total"] == 60
    assert result["count"] == 3
    assert "mean" in result
    assert "median" in result


def test_build_summary() -> None:
    metrics = {"total": 60, "mean": 20.0, "median": 20.0, "count": 3}
    result = build_summary.fn(metrics, "test-report")
    assert isinstance(result, str)
    assert "test-report" in result
    assert "60" in result


def test_transient_task() -> None:
    result = transient_task.fn()
    assert "transient" in result


def test_flow_runs() -> None:
    state = result_persistence_flow(return_state=True)
    assert state.is_completed()
