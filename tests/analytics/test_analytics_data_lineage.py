"""Tests for flow 097 -- Data Lineage Tracking."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "analytics_data_lineage",
    Path(__file__).resolve().parent.parent.parent / "flows" / "analytics" / "analytics_data_lineage.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["analytics_data_lineage"] = _mod
_spec.loader.exec_module(_mod)

DataRecord = _mod.DataRecord
LineageEntry = _mod.LineageEntry
LineageGraph = _mod.LineageGraph
LineageReport = _mod.LineageReport
compute_data_hash = _mod.compute_data_hash
record_lineage = _mod.record_lineage
ingest_with_lineage = _mod.ingest_with_lineage
transform_filter = _mod.transform_filter
transform_enrich = _mod.transform_enrich
transform_dedup = _mod.transform_dedup
build_lineage_graph = _mod.build_lineage_graph
lineage_report = _mod.lineage_report
data_lineage_flow = _mod.data_lineage_flow


def test_compute_data_hash() -> None:
    records = [DataRecord(id=1, name="a", value=10)]
    h = compute_data_hash.fn(records)
    assert isinstance(h, str)
    assert len(h) == 16


def test_compute_data_hash_deterministic() -> None:
    records = [DataRecord(id=1, name="a", value=10)]
    assert compute_data_hash.fn(records) == compute_data_hash.fn(records)


def test_hash_changes_with_data() -> None:
    h1 = compute_data_hash.fn([DataRecord(id=1, name="a", value=10)])
    h2 = compute_data_hash.fn([DataRecord(id=2, name="b", value=20)])
    assert h1 != h2


def test_ingest_preserves_data() -> None:
    data = [DataRecord(id=1, name="a", value=50)]
    result, entry = ingest_with_lineage.fn(data)
    assert result == data
    assert entry.input_hash == entry.output_hash  # Ingest doesn't modify


def test_filter_changes_hash() -> None:
    data = [DataRecord(id=1, name="a", value=50), DataRecord(id=2, name="b", value=5)]
    filtered, entry = transform_filter.fn(data, min_value=20)
    assert len(filtered) == 1
    assert entry.input_hash != entry.output_hash


def test_enrich_changes_hash() -> None:
    data = [DataRecord(id=1, name="a", value=50)]
    enriched, entry = transform_enrich.fn(data)
    assert enriched[0].quality_tier in {"high", "standard"}
    assert entry.input_hash != entry.output_hash


def test_dedup_changes_hash() -> None:
    data = [DataRecord(id=1, name="a", value=50), DataRecord(id=1, name="a", value=50)]
    deduped, entry = transform_dedup.fn(data, "id")
    assert len(deduped) == 1
    assert entry.input_hash != entry.output_hash


def test_lineage_graph_entry_count() -> None:
    entries = [
        record_lineage.fn("s1", "op1", "h1", "h2", 10),
        record_lineage.fn("s2", "op2", "h2", "h3", 8),
    ]
    graph = build_lineage_graph.fn(entries)
    assert len(graph.entries) == 2
    assert len(graph.stages) == 2


def test_flow_runs() -> None:
    state = data_lineage_flow(return_state=True)
    assert state.is_completed()
