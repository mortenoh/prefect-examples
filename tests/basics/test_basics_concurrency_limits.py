"""Tests for flow 016 — Concurrency Limits."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "basics_concurrency_limits",
    Path(__file__).resolve().parent.parent.parent / "flows" / "basics" / "basics_concurrency_limits.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["basics_concurrency_limits"] = _mod
_spec.loader.exec_module(_mod)

limited_task = _mod.limited_task
concurrency_limits_flow = _mod.concurrency_limits_flow


def test_limited_task_returns_string() -> None:
    result = limited_task.fn("item")
    assert isinstance(result, str)


def test_flow_runs() -> None:
    state = concurrency_limits_flow(return_state=True)
    assert state.is_completed()
