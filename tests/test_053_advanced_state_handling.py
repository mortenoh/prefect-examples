"""Tests for flow 053 -- Advanced State Handling."""

import importlib.util
import sys
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "flow_053",
    Path(__file__).resolve().parent.parent / "flows" / "053_advanced_state_handling.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_053"] = _mod
_spec.loader.exec_module(_mod)

succeed_task = _mod.succeed_task
fail_task = _mod.fail_task
skip_task = _mod.skip_task
inspect_states = _mod.inspect_states
advanced_state_handling_flow = _mod.advanced_state_handling_flow


def test_succeed_task() -> None:
    result = succeed_task.fn()
    assert result == "Task succeeded"


def test_fail_task() -> None:
    with pytest.raises(ValueError, match="Intentional failure"):
        fail_task.fn()


def test_skip_task_skips() -> None:
    result = skip_task.fn(should_skip=True)
    assert result == "skipped"


def test_skip_task_runs() -> None:
    result = skip_task.fn(should_skip=False)
    assert result == "completed"


def test_inspect_states() -> None:
    states = [{"name": "a", "status": "ok"}, {"name": "b", "status": "failed"}]
    result = inspect_states.fn(states)
    assert "2 tasks" in result


def test_flow_runs() -> None:
    state = advanced_state_handling_flow(return_state=True)
    assert state.is_completed()
