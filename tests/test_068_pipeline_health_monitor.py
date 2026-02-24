"""Tests for flow 068 -- Pipeline Health Monitor."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_068",
    Path(__file__).resolve().parent.parent / "flows" / "068_pipeline_health_monitor.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_068"] = _mod
_spec.loader.exec_module(_mod)

HealthCheck = _mod.HealthCheck
HealthCheckResult = _mod.HealthCheckResult
PipelineHealthReport = _mod.PipelineHealthReport
check_file_exists = _mod.check_file_exists
check_file_freshness = _mod.check_file_freshness
check_row_count = _mod.check_row_count
check_value_in_range = _mod.check_value_in_range
aggregate_health = _mod.aggregate_health
pipeline_health_monitor_flow = _mod.pipeline_health_monitor_flow


def test_health_check_model() -> None:
    hc = HealthCheck(name="test", check_type="file_exists", target="/tmp/x")
    assert hc.check_type == "file_exists"


def test_check_file_exists_true(tmp_path: Path) -> None:
    f = tmp_path / "exists.txt"
    f.write_text("data")
    result = check_file_exists.fn(str(f))
    assert result.status == "healthy"


def test_check_file_exists_false(tmp_path: Path) -> None:
    result = check_file_exists.fn(str(tmp_path / "missing.txt"))
    assert result.status == "critical"


def test_check_file_freshness(tmp_path: Path) -> None:
    f = tmp_path / "fresh.txt"
    f.write_text("data")
    result = check_file_freshness.fn(str(f), max_age_seconds=60)
    assert result.status == "healthy"


def test_check_row_count_healthy() -> None:
    data = [{"a": 1}, {"a": 2}]
    result = check_row_count.fn(data, min_rows=1, max_rows=10)
    assert result.status == "healthy"


def test_check_row_count_critical() -> None:
    result = check_row_count.fn([], min_rows=5, max_rows=10)
    assert result.status == "critical"


def test_check_value_in_range_healthy() -> None:
    data = [{"val": 10}, {"val": 50}]
    result = check_value_in_range.fn(data, "val", 0, 100)
    assert result.status == "healthy"


def test_aggregate_worst_status() -> None:
    results = [
        HealthCheckResult(check_name="a", status="healthy", details="ok"),
        HealthCheckResult(check_name="b", status="critical", details="bad"),
    ]
    report = aggregate_health.fn("test", results)
    assert report.overall_status == "critical"


def test_flow_runs(tmp_path: Path) -> None:
    state = pipeline_health_monitor_flow(work_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
