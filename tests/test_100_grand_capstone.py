"""Tests for flow 100 -- Grand Capstone."""

import csv
import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_100",
    Path(__file__).resolve().parent.parent / "flows" / "100_grand_capstone.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_100"] = _mod
_spec.loader.exec_module(_mod)

CapstoneRecord = _mod.CapstoneRecord
ProfileSummary = _mod.ProfileSummary
CapstoneStage = _mod.CapstoneStage
QualityResult = _mod.QualityResult
RegressionResult = _mod.RegressionResult
DimensionSummary = _mod.DimensionSummary
LineageEntry = _mod.LineageEntry
CapstoneResult = _mod.CapstoneResult
ingest_data = _mod.ingest_data
profile_data = _mod.profile_data
run_quality_checks = _mod.run_quality_checks
enrich_and_deduplicate = _mod.enrich_and_deduplicate
run_regression = _mod.run_regression
build_dimensions = _mod.build_dimensions
track_lineage = _mod.track_lineage
grand_capstone_flow = _mod.grand_capstone_flow


def _make_capstone_csv(path: Path) -> None:
    fieldnames = ["id", "name", "category", "value", "score", "spending"]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(1, 21):
            writer.writerow(
                {
                    "id": i,
                    "name": f"entity_{i}",
                    "category": ["health", "education", "infra"][i % 3],
                    "value": round(10.0 + i * 5.0, 1),
                    "score": round(50 + i * 2.0, 1),
                    "spending": round(100 + i * 30, 1),
                }
            )


def test_ingest_data(tmp_path: Path) -> None:
    path = tmp_path / "test.csv"
    _make_capstone_csv(path)
    records = ingest_data.fn(path)
    assert len(records) == 20
    assert all(isinstance(r, CapstoneRecord) for r in records)
    assert isinstance(records[0].value, float)


def test_profile_data() -> None:
    records = [
        CapstoneRecord(id=1, name="a", category="x", value=10.0, score=5.0, spending=100.0),
        CapstoneRecord(id=2, name="b", category="y", value=20.0, score=10.0, spending=200.0),
    ]
    profile = profile_data.fn(records)
    assert isinstance(profile, ProfileSummary)
    assert profile.row_count == 2
    assert profile.completeness > 0


def test_run_quality_checks() -> None:
    records = [
        CapstoneRecord(id=1, name="a", category="x", value=50.0, score=5.0, spending=100.0),
        CapstoneRecord(id=2, name="b", category="y", value=100.0, score=10.0, spending=200.0),
    ]
    result = run_quality_checks.fn(records)
    assert isinstance(result, QualityResult)
    assert result.score > 0


def test_enrich_and_deduplicate() -> None:
    records = [
        CapstoneRecord(id=1, name="a", category="x", value=50.0, score=5.0, spending=100.0),
        CapstoneRecord(id=1, name="a", category="x", value=50.0, score=5.0, spending=100.0),
        CapstoneRecord(id=2, name="b", category="y", value=200.0, score=10.0, spending=300.0),
    ]
    result = enrich_and_deduplicate.fn(records)
    assert len(result) == 2
    assert all(isinstance(r, CapstoneRecord) for r in result)
    assert result[0].value_tier in {"high", "low"}


def test_run_regression() -> None:
    records = [
        CapstoneRecord(id=i, name=f"e{i}", category="x", value=float(i), score=float(i * 2), spending=0.0)
        for i in range(1, 11)
    ]
    reg = run_regression.fn(records, "value", "score")
    assert isinstance(reg, RegressionResult)
    assert 0.0 <= reg.r_squared <= 1.0
    assert reg.r_squared > 0.99


def test_build_dimensions() -> None:
    records = [
        CapstoneRecord(id=1, name="a", category="health", value=50.0, score=5.0, spending=100.0, value_tier="low"),
        CapstoneRecord(id=2, name="b", category="edu", value=150.0, score=10.0, spending=200.0, value_tier="high"),
    ]
    dims = build_dimensions.fn(records)
    assert isinstance(dims, DimensionSummary)
    assert dims.fact_count == 2


def test_track_lineage() -> None:
    stages = [
        CapstoneStage(name="s1", status="completed", records_in=10, records_out=8, duration=0.1),
        CapstoneStage(name="s2", status="completed", records_in=8, records_out=7, duration=0.2),
    ]
    lineage = track_lineage.fn(stages)
    assert len(lineage) == 2
    assert all(isinstance(e, LineageEntry) for e in lineage)


def test_r_squared_range(tmp_path: Path) -> None:
    path = tmp_path / "test.csv"
    _make_capstone_csv(path)
    records = ingest_data.fn(path)
    reg = run_regression.fn(records, "value", "score")
    assert 0.0 <= reg.r_squared <= 1.0


def test_flow_runs(tmp_path: Path) -> None:
    state = grand_capstone_flow(work_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
