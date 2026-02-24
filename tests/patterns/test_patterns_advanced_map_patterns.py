"""Tests for flow 045 -- Advanced Map Patterns."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "patterns_advanced_map_patterns",
    Path(__file__).resolve().parent.parent.parent / "flows" / "patterns" / "patterns_advanced_map_patterns.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["patterns_advanced_map_patterns"] = _mod
_spec.loader.exec_module(_mod)

process_station = _mod.process_station
extract_data = _mod.extract_data
summarize_results = _mod.summarize_results
advanced_map_patterns_flow = _mod.advanced_map_patterns_flow


def test_process_station() -> None:
    result = process_station.fn("WX001", 40.7, -74.0)
    assert isinstance(result, dict)
    assert result["station_id"] == "WX001"
    assert "reading" in result


def test_extract_data() -> None:
    result = extract_data.fn("2024-01-01", "temperature")
    assert isinstance(result, dict)
    assert result["date"] == "2024-01-01"
    assert result["variable"] == "temperature"


def test_summarize_results() -> None:
    results = [{"a": 1}, {"b": 2}, {"c": 3}]
    summary = summarize_results.fn(results)
    assert "3" in summary


def test_flow_runs() -> None:
    state = advanced_map_patterns_flow(return_state=True)
    assert state.is_completed()
