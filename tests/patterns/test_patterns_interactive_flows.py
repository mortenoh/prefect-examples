"""Tests for flow 058 -- Interactive Flows."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "patterns_interactive_flows",
    Path(__file__).resolve().parent.parent.parent / "flows" / "patterns" / "patterns_interactive_flows.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["patterns_interactive_flows"] = _mod
_spec.loader.exec_module(_mod)

prepare_data = _mod.prepare_data
mock_approval = _mod.mock_approval
publish = _mod.publish
archive = _mod.archive
interactive_flows_flow = _mod.interactive_flows_flow


def test_prepare_data() -> None:
    result = prepare_data.fn()
    assert isinstance(result, dict)
    assert result["report"] == "Q4 Financial Summary"
    assert result["requires_approval"] is True


def test_mock_approval_approved() -> None:
    data = {"report": "Test", "total_revenue": 1000, "total_expenses": 500}
    assert mock_approval.fn(data) is True


def test_mock_approval_rejected() -> None:
    data = {"report": "Test", "total_revenue": 100, "total_expenses": 500}
    assert mock_approval.fn(data) is False


def test_publish() -> None:
    result = publish.fn({"report": "Test Report"})
    assert "Published" in result


def test_archive() -> None:
    result = archive.fn({"report": "Test Report"})
    assert "Archived" in result


def test_flow_runs() -> None:
    state = interactive_flows_flow(return_state=True)
    assert state.is_completed()
