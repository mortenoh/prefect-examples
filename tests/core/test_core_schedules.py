"""Tests for flow 038 — Schedules."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "core_schedules",
    Path(__file__).resolve().parent.parent.parent / "flows" / "core" / "core_schedules.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["core_schedules"] = _mod
_spec.loader.exec_module(_mod)

run_daily_report = _mod.run_daily_report
run_interval_check = _mod.run_interval_check
run_custom_schedule_job = _mod.run_custom_schedule_job
daily_report_flow = _mod.daily_report_flow
interval_check_flow = _mod.interval_check_flow
custom_schedule_flow = _mod.custom_schedule_flow


def test_run_daily_report() -> None:
    result = run_daily_report.fn("2025-01-15")
    assert isinstance(result, str)
    assert "2025-01-15" in result


def test_run_interval_check() -> None:
    result = run_interval_check.fn()
    assert isinstance(result, str)
    assert "OK" in result


def test_run_custom_schedule_job() -> None:
    result = run_custom_schedule_job.fn()
    assert isinstance(result, str)
    assert "completed" in result


def test_daily_report_flow() -> None:
    result = daily_report_flow("2025-06-01")
    assert result is None  # flow returns None


def test_interval_check_flow() -> None:
    result = interval_check_flow()
    assert result is None


def test_custom_schedule_flow() -> None:
    result = custom_schedule_flow()
    assert result is None
