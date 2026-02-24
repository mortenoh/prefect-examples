"""Tests for flow 055 -- Backfill Patterns."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_055",
    Path(__file__).resolve().parent.parent / "flows" / "055_backfill_patterns.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_055"] = _mod
_spec.loader.exec_module(_mod)

process_date = _mod.process_date
detect_gaps = _mod.detect_gaps
backfill_report = _mod.backfill_report
backfill_patterns_flow = _mod.backfill_patterns_flow


def test_process_date() -> None:
    result = process_date.fn("2024-01-01")
    assert isinstance(result, dict)
    assert result["date"] == "2024-01-01"
    assert result["status"] == "processed"


def test_detect_gaps() -> None:
    processed = ["2024-01-01", "2024-01-03"]
    gaps = detect_gaps.fn(processed, "2024-01-01", "2024-01-03")
    assert gaps == ["2024-01-02"]


def test_detect_gaps_none() -> None:
    processed = ["2024-01-01", "2024-01-02", "2024-01-03"]
    gaps = detect_gaps.fn(processed, "2024-01-01", "2024-01-03")
    assert gaps == []


def test_backfill_report() -> None:
    original = [{"date": "2024-01-01"}]
    backfilled = [{"date": "2024-01-02"}]
    result = backfill_report.fn(original, backfilled)
    assert "1 original" in result
    assert "1 backfilled" in result
    assert "2 total" in result


def test_flow_runs() -> None:
    state = backfill_patterns_flow(return_state=True)
    assert state.is_completed()
