"""Tests for flow 003 — Task Dependencies."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "basics_task_dependencies",
    Path(__file__).resolve().parent.parent.parent / "flows" / "basics" / "basics_task_dependencies.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["basics_task_dependencies"] = _mod
_spec.loader.exec_module(_mod)

start = _mod.start
task_a = _mod.task_a
task_b = _mod.task_b
task_c = _mod.task_c
join = _mod.join
task_dependencies_flow = _mod.task_dependencies_flow


def test_start() -> None:
    result = start.fn()
    assert isinstance(result, str)


def test_task_a() -> None:
    result = task_a.fn("input")
    assert isinstance(result, str)


def test_task_b() -> None:
    result = task_b.fn("input")
    assert isinstance(result, str)


def test_task_c() -> None:
    result = task_c.fn("input")
    assert isinstance(result, str)


def test_join() -> None:
    result = join.fn(["a", "b"])
    assert isinstance(result, str)
    assert "a" in result
    assert "b" in result


def test_flow_runs() -> None:
    state = task_dependencies_flow(return_state=True)
    assert state.is_completed()
