"""Tests for flow 021 — Task Caching."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "core_task_caching",
    Path(__file__).resolve().parent.parent.parent / "flows" / "core" / "core_task_caching.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["core_task_caching"] = _mod
_spec.loader.exec_module(_mod)

expensive_computation = _mod.expensive_computation
compound_cache_task = _mod.compound_cache_task
cached_lookup = _mod.cached_lookup
task_caching_flow = _mod.task_caching_flow


def test_expensive_computation() -> None:
    result = expensive_computation.fn(3, 7)
    assert result == 21


def test_expensive_computation_different_args() -> None:
    result = expensive_computation.fn(5, 4)
    assert result == 20


def test_compound_cache_task() -> None:
    result = compound_cache_task.fn("hello")
    assert result == "HELLO"


def test_cached_lookup() -> None:
    result = cached_lookup.fn("widgets", 42)
    assert isinstance(result, dict)
    assert result["category"] == "widgets"
    assert result["item_id"] == 42


def test_flow_runs() -> None:
    state = task_caching_flow(return_state=True)
    assert state.is_completed()
