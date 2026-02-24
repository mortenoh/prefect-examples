"""Tests for flow 006 — Conditional Logic."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "basics_conditional_logic",
    Path(__file__).resolve().parent.parent.parent / "flows" / "basics" / "basics_conditional_logic.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["basics_conditional_logic"] = _mod
_spec.loader.exec_module(_mod)

check_condition = _mod.check_condition
path_a = _mod.path_a
path_b = _mod.path_b
default_path = _mod.default_path
conditional_logic_flow = _mod.conditional_logic_flow


def test_check_condition() -> None:
    result = check_condition.fn()
    assert isinstance(result, str)


def test_path_a() -> None:
    result = path_a.fn()
    assert isinstance(result, str)


def test_path_b() -> None:
    result = path_b.fn()
    assert isinstance(result, str)


def test_default_path() -> None:
    result = default_path.fn()
    assert isinstance(result, str)


def test_flow_runs() -> None:
    state = conditional_logic_flow(return_state=True)
    assert state.is_completed()
