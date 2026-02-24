"""Tests for flow 023 — Task Run Names."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "core_task_run_names",
    Path(__file__).resolve().parent.parent.parent / "flows" / "core" / "core_task_run_names.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["core_task_run_names"] = _mod
_spec.loader.exec_module(_mod)

fetch_data = _mod.fetch_data
process_batch = _mod.process_batch
task_run_names_flow = _mod.task_run_names_flow


def test_fetch_data() -> None:
    result = fetch_data.fn("api", 1)
    assert isinstance(result, dict)
    assert result["source"] == "api"
    assert result["page"] == 1
    assert result["records"] == 10


def test_process_batch() -> None:
    result = process_batch.fn("us-east", 101)
    assert isinstance(result, str)
    assert "us-east" in result
    assert "101" in result


def test_flow_runs() -> None:
    state = task_run_names_flow(return_state=True)
    assert state.is_completed()
