"""Tests for flow 018 — Early Return."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "basics_early_return",
    Path(__file__).resolve().parent.parent.parent / "flows" / "basics" / "basics_early_return.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["basics_early_return"] = _mod
_spec.loader.exec_module(_mod)

should_continue = _mod.should_continue
do_work = _mod.do_work
do_more_work = _mod.do_more_work
early_return_flow = _mod.early_return_flow


def test_should_continue_returns_bool() -> None:
    result = should_continue.fn()
    assert isinstance(result, bool)


def test_flow_short_circuits() -> None:
    state = early_return_flow(skip=True, return_state=True)
    assert state.is_completed()


def test_flow_continues() -> None:
    state = early_return_flow(skip=False, return_state=True)
    assert state.is_completed()
