"""Tests for flow 037 — Flow Serve."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "core_flow_serve",
    Path(__file__).resolve().parent.parent.parent / "flows" / "core" / "core_flow_serve.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["core_flow_serve"] = _mod
_spec.loader.exec_module(_mod)

extract_data = _mod.extract_data
transform_data = _mod.transform_data
load_data = _mod.load_data
flow_serve_flow = _mod.flow_serve_flow


def test_extract_data() -> None:
    result = extract_data.fn()
    assert isinstance(result, list)
    assert len(result) == 3
    assert "value" in result[0]


def test_transform_data() -> None:
    records = [{"id": 1, "value": 100}, {"id": 2, "value": 200}]
    result = transform_data.fn(records)
    assert len(result) == 2
    assert "normalised" in result[0]
    assert result[1]["normalised"] == 1.0  # max value


def test_load_data() -> None:
    records = [{"id": 1, "value": 100, "normalised": 0.5}]
    result = load_data.fn(records)
    assert isinstance(result, str)
    assert "1" in result


def test_flow_runs() -> None:
    state = flow_serve_flow(return_state=True)
    assert state.is_completed()
