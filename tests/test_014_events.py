"""Tests for flow 014 — Events."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "flow_014",
    Path(__file__).resolve().parent.parent / "flows" / "014_events.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_014"] = _mod
_spec.loader.exec_module(_mod)

produce_data = _mod.produce_data
events_flow = _mod.events_flow


def test_produce_data() -> None:
    result = produce_data.fn()
    assert isinstance(result, str)
    assert len(result) > 0


def test_flow_runs() -> None:
    state = events_flow(return_state=True)
    assert state.is_completed()
