"""Tests for flow 060 -- Production Pipeline v2."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "patterns_production_pipeline_v2",
    Path(__file__).resolve().parent.parent.parent / "flows" / "patterns" / "patterns_production_pipeline_v2.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["patterns_production_pipeline_v2"] = _mod
_spec.loader.exec_module(_mod)

SourceRecord = _mod.SourceRecord
ValidatedRecord = _mod.ValidatedRecord
PipelineMetrics = _mod.PipelineMetrics
extract_record = _mod.extract_record
validate_record = _mod.validate_record
transform_record = _mod.transform_record
compute_metrics = _mod.compute_metrics
publish_summary = _mod.publish_summary
extract_stage = _mod.extract_stage
validate_stage = _mod.validate_stage
transform_stage = _mod.transform_stage


def test_source_record() -> None:
    record = SourceRecord(id=1, name="Alice", value=100.0, region="US")
    assert record.name == "Alice"
    assert record.value == 100.0


def test_validated_record() -> None:
    record = ValidatedRecord(id=1, name="Alice", value=100.0, region="US")
    assert record.valid is True


def test_validated_record_rejects_negative() -> None:
    import pytest

    with pytest.raises(ValueError):
        ValidatedRecord(id=1, name="Alice", value=-50.0, region="US")


def test_extract_record() -> None:
    data = {"id": 1, "name": "Alice", "value": 100.0, "region": "US"}
    result = extract_record.fn(data)
    assert isinstance(result, SourceRecord)
    assert result.id == 1


def test_validate_record_valid() -> None:
    record = SourceRecord(id=1, name="Alice", value=100.0, region="US")
    result = validate_record.fn(record)
    assert isinstance(result, ValidatedRecord)


def test_transform_record() -> None:
    record = ValidatedRecord(id=1, name="Alice", value=250.0, region="US")
    result = transform_record.fn(record)
    assert isinstance(result, dict)
    assert result["value_category"] == "high"
    assert "processed_at" in result


def test_compute_metrics() -> None:
    records = [
        {"id": 1, "value": 100, "valid": True, "region": "US"},
        {"id": 2, "value": 200, "valid": True, "region": "EU"},
    ]
    metrics = compute_metrics.fn(records)
    assert isinstance(metrics, PipelineMetrics)
    assert metrics.total_records == 2
    assert metrics.valid_records == 2


def test_extract_stage() -> None:
    result = extract_stage()
    assert isinstance(result, list)
    assert len(result) == 5


def test_transform_stage() -> None:
    records = [ValidatedRecord(id=1, name="Alice", value=100.0, region="US")]
    result = transform_stage(records)
    assert len(result) == 1
    assert "processed_at" in result[0]
