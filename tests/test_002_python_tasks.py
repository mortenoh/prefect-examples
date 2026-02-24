"""Tests for flow 002 — Python Tasks."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "flow_002",
    Path(__file__).resolve().parent.parent / "flows" / "002_python_tasks.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_002"] = _mod
_spec.loader.exec_module(_mod)

greet = _mod.greet
compute_sum = _mod.compute_sum
python_tasks_flow = _mod.python_tasks_flow


def test_greet_returns_string() -> None:
    result = greet.fn("Alice")
    assert isinstance(result, str)
    assert "Alice" in result


def test_compute_sum() -> None:
    result = compute_sum.fn(3, 7)
    assert result == 10


def test_flow_runs() -> None:
    state = python_tasks_flow(return_state=True)
    assert state.is_completed()
