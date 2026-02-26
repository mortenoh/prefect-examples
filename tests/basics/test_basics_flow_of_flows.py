"""Tests for flow 015 -- Flow of Flows."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally -- use importlib.
_spec = importlib.util.spec_from_file_location(
    "basics_flow_of_flows",
    Path(__file__).resolve().parent.parent.parent / "flows" / "basics" / "basics_flow_of_flows.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["basics_flow_of_flows"] = _mod
_spec.loader.exec_module(_mod)

RawData = _mod.RawData
Record = _mod.Record
ProcessedData = _mod.ProcessedData
ingest_flow = _mod.ingest_flow
transform_flow = _mod.transform_flow
report_flow = _mod.report_flow


def test_ingest_flow_returns_raw_data() -> None:
    result = ingest_flow()
    assert isinstance(result, RawData)
    assert len(result.records) > 0


def test_transform_flow_returns_processed_data() -> None:
    raw = RawData(source="test", records=[Record(id=1, value=10)])
    result = transform_flow(raw)
    assert isinstance(result, ProcessedData)
    assert result.record_count == 1


def test_report_flow_returns_string() -> None:
    data = ProcessedData(source="test", record_count=1, total_value=10, records=[])
    result = report_flow(data)
    assert isinstance(result, str)
    assert "Report" in result


def test_orchestrator_end_to_end() -> None:
    """Test the full pipeline by calling subflows in sequence."""
    raw = ingest_flow()
    processed = transform_flow(raw)
    summary = report_flow(processed)
    assert isinstance(summary, str)
    assert "Report" in summary
