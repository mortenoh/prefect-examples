"""Tests for flow 039 — Work Pools."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "core_work_pools",
    Path(__file__).resolve().parent.parent.parent / "flows" / "core" / "core_work_pools.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["core_work_pools"] = _mod
_spec.loader.exec_module(_mod)

fetch_data = _mod.fetch_data
process_data = _mod.process_data
save_results = _mod.save_results
work_pools_flow = _mod.work_pools_flow


def test_fetch_data() -> None:
    result = fetch_data.fn("test-source")
    assert isinstance(result, dict)
    assert result["source"] == "test-source"
    assert len(result["records"]) == 5


def test_process_data() -> None:
    data = {"source": "test", "records": [{"id": 1, "value": 10}, {"id": 2, "value": 20}]}
    result = process_data.fn(data)
    assert isinstance(result, str)
    assert "30" in result  # total


def test_save_results() -> None:
    result = save_results.fn("test-result")
    assert isinstance(result, str)
    assert "Saved" in result


def test_flow_runs() -> None:
    state = work_pools_flow(return_state=True)
    assert state.is_completed()
