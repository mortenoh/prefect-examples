"""Tests for flow 044 -- Task Factories."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_044",
    Path(__file__).resolve().parent.parent / "flows" / "044_task_factories.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_044"] = _mod
_spec.loader.exec_module(_mod)

make_extractor = _mod.make_extractor
extract_api = _mod.extract_api
extract_database = _mod.extract_database
extract_file = _mod.extract_file
combine_extracts = _mod.combine_extracts
task_factories_flow = _mod.task_factories_flow


def test_make_extractor_creates_task() -> None:
    extractor = make_extractor("test_source")
    result = extractor.fn()
    assert isinstance(result, dict)
    assert result["source"] == "test_source"
    assert len(result["records"]) == 3


def test_extract_api() -> None:
    result = extract_api.fn()
    assert result["source"] == "api"


def test_extract_database() -> None:
    result = extract_database.fn()
    assert result["source"] == "database"


def test_extract_file() -> None:
    result = extract_file.fn()
    assert result["source"] == "file"


def test_combine_extracts() -> None:
    extracts = [
        {"source": "api", "records": ["r1", "r2"]},
        {"source": "db", "records": ["r3"]},
    ]
    result = combine_extracts.fn(extracts)
    assert "3 records" in result
    assert "api" in result


def test_flow_runs() -> None:
    state = task_factories_flow(return_state=True)
    assert state.is_completed()
