"""Tests for flow 091 -- Staged ETL Pipeline."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_091",
    Path(__file__).resolve().parent.parent / "flows" / "091_staged_etl.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_091"] = _mod
_spec.loader.exec_module(_mod)

RawRecord = _mod.RawRecord
StagingRecord = _mod.StagingRecord
ProductionRecord = _mod.ProductionRecord
SummaryStats = _mod.SummaryStats
EtlResult = _mod.EtlResult
generate_raw_data = _mod.generate_raw_data
load_staging = _mod.load_staging
validate_and_transform = _mod.validate_and_transform
filter_valid = _mod.filter_valid
compute_summary = _mod.compute_summary
staged_etl_flow = _mod.staged_etl_flow


def test_generate_raw_data() -> None:
    data = generate_raw_data.fn()
    assert len(data) == 10
    assert all(isinstance(d, RawRecord) for d in data)


def test_load_staging() -> None:
    raw = generate_raw_data.fn()
    staging = load_staging.fn(raw)
    assert len(staging) == 10
    assert all(isinstance(s, StagingRecord) for s in staging)
    assert all(s.load_timestamp != "" for s in staging)


def test_validate_and_transform_detects_invalid() -> None:
    raw = generate_raw_data.fn()
    staging = load_staging.fn(raw)
    production = validate_and_transform.fn(staging)
    invalid = [r for r in production if not r.is_valid]
    # "bad_value", "-50.0" (negative), "9999.0" (out of range), "" (empty)
    assert len(invalid) >= 3


def test_validate_valid_records() -> None:
    raw = generate_raw_data.fn()
    staging = load_staging.fn(raw)
    production = validate_and_transform.fn(staging)
    valid = [r for r in production if r.is_valid]
    assert all(0 <= r.value <= 1000 for r in valid)


def test_filter_valid() -> None:
    raw = generate_raw_data.fn()
    staging = load_staging.fn(raw)
    production = validate_and_transform.fn(staging)
    valid = filter_valid.fn(production)
    assert all(r.is_valid for r in valid)
    assert len(valid) < len(production)


def test_compute_summary() -> None:
    raw = generate_raw_data.fn()
    staging = load_staging.fn(raw)
    production = validate_and_transform.fn(staging)
    valid = filter_valid.fn(production)
    summary = compute_summary.fn(valid, "category")
    assert len(summary) > 0
    for s in summary:
        assert s.min_val <= s.avg <= s.max_val
        assert s.count > 0


etl_report = _mod.etl_report


def test_etl_result_counts() -> None:
    raw = generate_raw_data.fn()
    staging = load_staging.fn(raw)
    production = validate_and_transform.fn(staging)
    valid = filter_valid.fn(production)
    summary = compute_summary.fn(valid, "category")
    result = etl_report.fn(len(staging), production, summary)
    assert result.staging_count == 10
    assert result.valid_count + result.invalid_count == result.production_count


def test_flow_runs() -> None:
    state = staged_etl_flow(return_state=True)
    assert state.is_completed()
