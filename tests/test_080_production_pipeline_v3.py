"""Tests for flow 080 -- Production Pipeline v3."""

import csv
import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_080",
    Path(__file__).resolve().parent.parent / "flows" / "080_production_pipeline_v3.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_080"] = _mod
_spec.loader.exec_module(_mod)

PipelineStage = _mod.PipelineStage
PipelineV3Result = _mod.PipelineV3Result
ingest_csv = _mod.ingest_csv
profile_data = _mod.profile_data
run_quality_checks = _mod.run_quality_checks
enrich_records = _mod.enrich_records
deduplicate_records = _mod.deduplicate_records
write_output = _mod.write_output
build_dashboard = _mod.build_dashboard
production_pipeline_v3_flow = _mod.production_pipeline_v3_flow


def _make_csv(path: Path, rows: int = 10) -> None:
    fieldnames = ["id", "name", "value", "score"]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(1, rows + 1):
            writer.writerow({"id": i, "name": f"item_{i}", "value": i * 10.0, "score": 50.0 + i})


def test_ingest_csv(tmp_path: Path) -> None:
    path = tmp_path / "test.csv"
    _make_csv(path, rows=5)
    records = ingest_csv.fn(path)
    assert len(records) == 5
    assert isinstance(records[0]["value"], float)


def test_profile_data() -> None:
    records = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    profile = profile_data.fn(records)
    assert profile["row_count"] == 2
    assert profile["completeness"] > 0


def test_run_quality_checks() -> None:
    records = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]
    rules = [{"type": "not_null", "column": "name"}, {"type": "unique", "column": "id"}]
    result = run_quality_checks.fn(records, rules)
    assert result["traffic_light"] == "green"


def test_enrich_records() -> None:
    records = [{"id": 1}, {"id": 2}]
    enriched, stats = enrich_records.fn(records, {})
    assert len(enriched) == 2
    assert "tier" in enriched[0]
    assert stats["misses"] == 2


def test_enrich_records_cache_hit() -> None:
    records = [{"id": 1}]
    cache: dict = {}
    enrich_records.fn(records, cache)
    _, stats = enrich_records.fn(records, cache)
    assert stats["hits"] == 1


def test_deduplicate_records() -> None:
    records = [{"id": 1, "name": "A"}, {"id": 1, "name": "A"}, {"id": 2, "name": "B"}]
    deduped = deduplicate_records.fn(records, ["id", "name"])
    assert len(deduped) == 2


def test_write_output(tmp_path: Path) -> None:
    records = [{"a": 1, "b": 2}]
    path = tmp_path / "out.csv"
    write_output.fn(records, path)
    assert path.exists()


def test_pipeline_stage_model() -> None:
    s = PipelineStage(name="test", status="completed", records_in=10, records_out=8, duration_seconds=0.5)
    assert s.records_out == 8


def test_flow_runs(tmp_path: Path) -> None:
    state = production_pipeline_v3_flow(work_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
