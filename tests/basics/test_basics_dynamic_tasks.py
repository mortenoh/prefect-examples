"""Tests for flow 010 — Dynamic Tasks."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "basics_dynamic_tasks",
    Path(__file__).resolve().parent.parent.parent / "flows" / "basics" / "basics_dynamic_tasks.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["basics_dynamic_tasks"] = _mod
_spec.loader.exec_module(_mod)

generate_items = _mod.generate_items
process_item = _mod.process_item
summarize = _mod.summarize
dynamic_tasks_flow = _mod.dynamic_tasks_flow


def test_generate_items() -> None:
    result = generate_items.fn()
    assert isinstance(result, list)
    assert len(result) > 0


def test_process_item() -> None:
    result = process_item.fn("item-x")
    assert isinstance(result, str)
    assert "item-x" in result


def test_summarize() -> None:
    result = summarize.fn(["a", "b", "c"])
    assert isinstance(result, str)
    assert "a" in result


def test_flow_runs() -> None:
    state = dynamic_tasks_flow(return_state=True)
    assert state.is_completed()
