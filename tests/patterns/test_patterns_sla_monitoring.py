"""Tests for flow 048 -- SLA Monitoring."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "patterns_sla_monitoring",
    Path(__file__).resolve().parent.parent.parent / "flows" / "patterns" / "patterns_sla_monitoring.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["patterns_sla_monitoring"] = _mod
_spec.loader.exec_module(_mod)

fast_task = _mod.fast_task
medium_task = _mod.medium_task
slow_task = _mod.slow_task
sla_report = _mod.sla_report
sla_monitoring_flow = _mod.sla_monitoring_flow


def test_fast_task() -> None:
    result = fast_task.fn()
    assert isinstance(result, dict)
    assert result["task"] == "fast_task"
    assert "duration" in result


def test_medium_task() -> None:
    result = medium_task.fn()
    assert result["task"] == "medium_task"


def test_slow_task() -> None:
    result = slow_task.fn()
    assert result["task"] == "slow_task"


def test_sla_report_no_breaches() -> None:
    results = [
        {"task": "fast_task", "duration": 0.01},
        {"task": "medium_task", "duration": 0.05},
    ]
    report = sla_report.fn(results)
    assert "OK" in report
    assert "breaches: 0" in report


def test_sla_report_with_breach() -> None:
    results = [{"task": "fast_task", "duration": 1.5}]
    thresholds = {"fast_task": 0.5}
    report = sla_report.fn(results, thresholds)
    assert "BREACH" in report
    assert "breaches: 1" in report


def test_flow_runs() -> None:
    state = sla_monitoring_flow(return_state=True)
    assert state.is_completed()
