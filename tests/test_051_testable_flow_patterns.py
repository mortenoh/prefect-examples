"""Tests for flow 051 -- Testable Flow Patterns."""

import importlib.util
import sys
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "flow_051",
    Path(__file__).resolve().parent.parent / "flows" / "051_testable_flow_patterns.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_051"] = _mod
_spec.loader.exec_module(_mod)

# Pure functions (no Prefect dependency)
_extract_records = _mod._extract_records
_validate_record = _mod._validate_record
_transform_record = _mod._transform_record

# Task wrappers
extract = _mod.extract
validate = _mod.validate
transform = _mod.transform
testable_flow_patterns_flow = _mod.testable_flow_patterns_flow


# --- Pure function tests (fast, no Prefect overhead) ---


def test_extract_records_pure() -> None:
    records = _extract_records()
    assert isinstance(records, list)
    assert len(records) == 3


def test_validate_record_pure() -> None:
    result = _validate_record({"id": 1, "name": "Alice", "score": 88})
    assert result["valid"] is True


def test_validate_record_missing_name() -> None:
    with pytest.raises(ValueError, match="missing name"):
        _validate_record({"id": 1, "name": "", "score": 88})


def test_transform_record_pure() -> None:
    result = _transform_record({"id": 1, "name": "Alice", "score": 95, "valid": True})
    assert result["grade"] == "A"


def test_transform_record_grades() -> None:
    assert _transform_record({"score": 95})["grade"] == "A"
    assert _transform_record({"score": 85})["grade"] == "B"
    assert _transform_record({"score": 75})["grade"] == "C"
    assert _transform_record({"score": 50})["grade"] == "F"


# --- Task wrapper tests ---


def test_extract_task() -> None:
    result = extract.fn()
    assert isinstance(result, list)
    assert len(result) == 3


def test_validate_task() -> None:
    result = validate.fn({"id": 1, "name": "Alice", "score": 88})
    assert result["valid"] is True


def test_transform_task() -> None:
    result = transform.fn({"id": 1, "name": "Alice", "score": 88, "valid": True})
    assert result["grade"] == "B"


def test_flow_runs() -> None:
    state = testable_flow_patterns_flow(return_state=True)
    assert state.is_completed()
