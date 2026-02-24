"""Tests for flow 026 — Tags."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "flow_026",
    Path(__file__).resolve().parent.parent / "flows" / "026_tags.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_026"] = _mod
_spec.loader.exec_module(_mod)

extract_sales = _mod.extract_sales
transform_sales = _mod.transform_sales
generic_task = _mod.generic_task
tags_flow = _mod.tags_flow


def test_extract_sales() -> None:
    result = extract_sales.fn()
    assert isinstance(result, list)
    assert len(result) == 3
    assert "product" in result[0]


def test_transform_sales() -> None:
    records = [{"id": 1, "product": "A", "amount": 100.0}]
    result = transform_sales.fn(records)
    assert len(result) == 1
    assert "tax" in result[0]
    assert result[0]["tax"] == 10.0


def test_generic_task() -> None:
    result = generic_task.fn("test-data")
    assert "test-data" in result


def test_flow_runs() -> None:
    state = tags_flow(return_state=True)
    assert state.is_completed()
