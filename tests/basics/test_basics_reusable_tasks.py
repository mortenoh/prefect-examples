"""Tests for flow 013 — Reusable Tasks."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "basics_reusable_tasks",
    Path(__file__).resolve().parent.parent.parent / "flows" / "basics" / "basics_reusable_tasks.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["basics_reusable_tasks"] = _mod
_spec.loader.exec_module(_mod)

reusable_tasks_flow = _mod.reusable_tasks_flow


def test_flow_runs() -> None:
    state = reusable_tasks_flow(return_state=True)
    assert state.is_completed()
