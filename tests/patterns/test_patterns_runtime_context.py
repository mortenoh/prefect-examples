"""Tests for flow 056 -- Runtime Context."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "patterns_runtime_context",
    Path(__file__).resolve().parent.parent.parent / "flows" / "patterns" / "patterns_runtime_context.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["patterns_runtime_context"] = _mod
_spec.loader.exec_module(_mod)

get_task_info = _mod.get_task_info
get_flow_info = _mod.get_flow_info
get_parameters = _mod.get_parameters
runtime_context_flow = _mod.runtime_context_flow


def test_get_task_info() -> None:
    result = get_task_info.fn()
    assert isinstance(result, dict)
    assert "task_run_name" in result


def test_get_flow_info() -> None:
    result = get_flow_info.fn()
    assert isinstance(result, dict)
    assert "flow_run_name" in result


def test_get_parameters() -> None:
    result = get_parameters.fn()
    assert isinstance(result, dict)


def test_flow_runs() -> None:
    state = runtime_context_flow(return_state=True)
    assert state.is_completed()
