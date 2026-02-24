"""Tests for flow 096 -- Parallel Multi-Endpoint Export."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "analytics_parallel_export",
    Path(__file__).resolve().parent.parent.parent / "flows" / "analytics" / "analytics_parallel_export.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["analytics_parallel_export"] = _mod
_spec.loader.exec_module(_mod)

EndpointResult = _mod.EndpointResult
ExportSummary = _mod.ExportSummary
fetch_endpoint_a = _mod.fetch_endpoint_a
fetch_endpoint_b = _mod.fetch_endpoint_b
fetch_endpoint_c = _mod.fetch_endpoint_c
combine_results = _mod.combine_results
parallel_export_flow = _mod.parallel_export_flow


def test_fetch_endpoint_a(tmp_path: Path) -> None:
    result = fetch_endpoint_a.fn(str(tmp_path))
    assert isinstance(result, EndpointResult)
    assert result.record_count == 15
    assert result.output_format == "csv"
    assert Path(result.output_path).exists()


def test_fetch_endpoint_b(tmp_path: Path) -> None:
    result = fetch_endpoint_b.fn(str(tmp_path))
    assert isinstance(result, EndpointResult)
    assert result.record_count == 20
    assert result.output_format == "json"
    assert Path(result.output_path).exists()


def test_fetch_endpoint_c(tmp_path: Path) -> None:
    result = fetch_endpoint_c.fn(str(tmp_path))
    assert isinstance(result, EndpointResult)
    assert result.record_count == 10
    assert result.output_format == "csv"
    assert Path(result.output_path).exists()


def test_combine_results() -> None:
    results = [
        EndpointResult(endpoint="a", record_count=10, output_format="csv", output_path="/tmp/a.csv"),
        EndpointResult(endpoint="b", record_count=20, output_format="json", output_path="/tmp/b.json"),
        EndpointResult(endpoint="c", record_count=5, output_format="csv", output_path="/tmp/c.csv"),
    ]
    summary = combine_results.fn(results, 1.5)
    assert summary.total_records == 35
    assert summary.format_counts["csv"] == 2
    assert summary.format_counts["json"] == 1


def test_format_counts(tmp_path: Path) -> None:
    a = fetch_endpoint_a.fn(str(tmp_path))
    b = fetch_endpoint_b.fn(str(tmp_path))
    c = fetch_endpoint_c.fn(str(tmp_path))
    summary = combine_results.fn([a, b, c], 0.1)
    assert summary.format_counts["csv"] == 2
    assert summary.format_counts["json"] == 1


def test_total_records(tmp_path: Path) -> None:
    a = fetch_endpoint_a.fn(str(tmp_path))
    b = fetch_endpoint_b.fn(str(tmp_path))
    c = fetch_endpoint_c.fn(str(tmp_path))
    summary = combine_results.fn([a, b, c], 0.1)
    assert summary.total_records == 45  # 15 + 20 + 10


def test_flow_runs(tmp_path: Path) -> None:
    state = parallel_export_flow(output_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
