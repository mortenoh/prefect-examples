"""Tests for flow 008 — Parameterized Flows."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "flow_008",
    Path(__file__).resolve().parent.parent / "flows" / "008_parameterized_flows.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_008"] = _mod
_spec.loader.exec_module(_mod)

build_greeting = _mod.build_greeting
parameterized_flow = _mod.parameterized_flow


def test_build_greeting() -> None:
    result = build_greeting.fn("Alice", "2024-01-01", "Hello, {name}! Date: {date}.")
    assert isinstance(result, str)
    assert "Alice" in result


def test_flow_with_defaults() -> None:
    state = parameterized_flow(return_state=True)
    assert state.is_completed()


def test_flow_with_custom_params() -> None:
    state = parameterized_flow(name="Alice", return_state=True)
    assert state.is_completed()
