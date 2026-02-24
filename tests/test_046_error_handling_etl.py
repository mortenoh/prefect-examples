"""Tests for flow 046 -- Error Handling ETL."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_046",
    Path(__file__).resolve().parent.parent / "flows" / "046_error_handling_etl.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_046"] = _mod
_spec.loader.exec_module(_mod)

QuarantineResult = _mod.QuarantineResult
generate_data = _mod.generate_data
process_with_quarantine = _mod.process_with_quarantine
report_quarantine = _mod.report_quarantine
error_handling_etl_flow = _mod.error_handling_etl_flow


def test_generate_data() -> None:
    data = generate_data.fn()
    assert isinstance(data, list)
    assert len(data) == 5


def test_process_with_quarantine() -> None:
    records = [
        {"id": 1, "name": "Alice", "value": 100},
        {"id": 2, "name": "", "value": 50},
        {"id": 3, "name": "Charlie", "value": -10},
    ]
    result = process_with_quarantine.fn(records)
    assert isinstance(result, QuarantineResult)
    assert len(result.good_records) == 1
    assert len(result.bad_records) == 2
    assert len(result.errors) == 2


def test_report_quarantine() -> None:
    result = QuarantineResult(
        good_records=[{"id": 1}],
        bad_records=[{"id": 2}],
        errors=["Record 2: missing name"],
    )
    report = report_quarantine.fn(result)
    assert "1 good" in report
    assert "1 bad" in report


def test_flow_runs() -> None:
    state = error_handling_etl_flow(return_state=True)
    assert state.is_completed()
