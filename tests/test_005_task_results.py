"""Tests for flow 005 — Task Results."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "flow_005",
    Path(__file__).resolve().parent.parent / "flows" / "005_task_results.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_005"] = _mod
_spec.loader.exec_module(_mod)

produce_metrics = _mod.produce_metrics
consume_metrics = _mod.consume_metrics
task_results_flow = _mod.task_results_flow


def test_produce_metrics_keys() -> None:
    result = produce_metrics.fn()
    assert "total" in result
    assert "average" in result
    assert "items" in result


def test_consume_metrics() -> None:
    metrics = produce_metrics.fn()
    result = consume_metrics.fn(metrics)
    assert isinstance(result, str)


def test_flow_runs() -> None:
    state = task_results_flow(return_state=True)
    assert state.is_completed()
