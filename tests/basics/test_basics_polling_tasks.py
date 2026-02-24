"""Tests for flow 011 — Polling Tasks."""

import importlib.util
import sys
from pathlib import Path

import pytest

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "basics_polling_tasks",
    Path(__file__).resolve().parent.parent.parent / "flows" / "basics" / "basics_polling_tasks.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["basics_polling_tasks"] = _mod
_spec.loader.exec_module(_mod)

poll_condition = _mod.poll_condition
process_after_poll = _mod.process_after_poll
polling_flow = _mod.polling_flow


def test_poll_condition_succeeds() -> None:
    result = poll_condition.fn("test", succeed_after=0.1, timeout=2.0)
    assert isinstance(result, str)
    assert "Condition met" in result


def test_poll_condition_timeout() -> None:
    with pytest.raises(TimeoutError):
        poll_condition.fn("test", succeed_after=100, timeout=0.1)


def test_flow_runs() -> None:
    state = polling_flow(return_state=True)
    assert state.is_completed()
