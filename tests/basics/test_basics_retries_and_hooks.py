"""Tests for flow 012 — Retries and Hooks."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "basics_retries_and_hooks",
    Path(__file__).resolve().parent.parent.parent / "flows" / "basics" / "basics_retries_and_hooks.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["basics_retries_and_hooks"] = _mod
_spec.loader.exec_module(_mod)

flaky_task = _mod.flaky_task
reliable_task = _mod.reliable_task
retries_flow = _mod.retries_flow


def test_flaky_task_eventually_succeeds() -> None:
    state = retries_flow(return_state=True)
    assert state.is_completed()


def test_reliable_task() -> None:
    result = reliable_task.fn()
    assert isinstance(result, str)
    assert "reliable_task" in result


def test_flow_runs() -> None:
    state = retries_flow(return_state=True)
    assert state.is_completed()
