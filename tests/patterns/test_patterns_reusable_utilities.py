"""Tests for flow 052 -- Reusable Utilities."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "patterns_reusable_utilities",
    Path(__file__).resolve().parent.parent.parent / "flows" / "patterns" / "patterns_reusable_utilities.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["patterns_reusable_utilities"] = _mod
_spec.loader.exec_module(_mod)

MetricResult = _mod.MetricResult
compute_metric = _mod.compute_metric
produce_metric = _mod.produce_metric
report_metrics = _mod.report_metrics
reusable_utilities_flow = _mod.reusable_utilities_flow


def test_compute_metric() -> None:
    result = compute_metric.fn("throughput", 1000.0)
    assert isinstance(result, dict)
    assert result["name"] == "throughput"
    assert "_duration" in result


def test_produce_metric() -> None:
    result = produce_metric.fn("error_rate", 0.02, "percent")
    assert isinstance(result, MetricResult)
    assert result.name == "error_rate"
    assert result.value == 0.02


def test_report_metrics() -> None:
    result = report_metrics.fn([{"a": 1}, {"b": 2}])
    assert "2 metrics" in result


def test_flow_runs() -> None:
    state = reusable_utilities_flow(return_state=True)
    assert state.is_completed()
