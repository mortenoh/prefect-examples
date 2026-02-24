"""Tests for flow 007 — State Handlers."""

import importlib.util
import sys
from pathlib import Path

import pytest

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "basics_state_handlers",
    Path(__file__).resolve().parent.parent.parent / "flows" / "basics" / "basics_state_handlers.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["basics_state_handlers"] = _mod
_spec.loader.exec_module(_mod)

succeed_task = _mod.succeed_task
fail_task = _mod.fail_task
always_run_task = _mod.always_run_task
state_handlers_flow = _mod.state_handlers_flow


def test_succeed_task() -> None:
    result = succeed_task.fn()
    assert isinstance(result, str)


def test_fail_task_raises() -> None:
    with pytest.raises(ValueError):
        fail_task.fn()


def test_flow_completes_despite_failure() -> None:
    state = state_handlers_flow(return_state=True)
    assert state.is_completed()
