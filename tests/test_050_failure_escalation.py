"""Tests for flow 050 -- Failure Escalation."""

import importlib.util
import sys
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "flow_050",
    Path(__file__).resolve().parent.parent / "flows" / "050_failure_escalation.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_050"] = _mod
_spec.loader.exec_module(_mod)

flaky_task = _mod.flaky_task
stable_task = _mod.stable_task
failure_escalation_flow = _mod.failure_escalation_flow
_attempt_counter = _mod._attempt_counter


def test_stable_task() -> None:
    result = stable_task.fn()
    assert result == "stable_task completed"


def test_flaky_task_succeeds_immediately() -> None:
    _attempt_counter.clear()
    result = flaky_task.fn(fail_count=0)
    assert "succeeded" in result


def test_flaky_task_fails() -> None:
    _attempt_counter.clear()
    with pytest.raises(ValueError, match="simulated failure"):
        flaky_task.fn(fail_count=5)


def test_flow_runs() -> None:
    state = failure_escalation_flow(return_state=True)
    assert state.is_completed()
