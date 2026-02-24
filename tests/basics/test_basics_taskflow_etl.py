"""Tests for flow 004 — Taskflow / ETL."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "basics_taskflow_etl",
    Path(__file__).resolve().parent.parent.parent / "flows" / "basics" / "basics_taskflow_etl.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["basics_taskflow_etl"] = _mod
_spec.loader.exec_module(_mod)

extract = _mod.extract
transform = _mod.transform
load = _mod.load
taskflow_etl_flow = _mod.taskflow_etl_flow


def test_extract_returns_dict() -> None:
    result = extract.fn()
    assert isinstance(result, dict)


def test_transform() -> None:
    raw = extract.fn()
    result = transform.fn(raw)
    assert "users" in result
    assert "timestamp" in result


def test_load() -> None:
    raw = extract.fn()
    transformed = transform.fn(raw)
    result = load.fn(transformed)
    assert isinstance(result, str)


def test_flow_runs() -> None:
    state = taskflow_etl_flow(return_state=True)
    assert state.is_completed()
