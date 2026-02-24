"""Tests for flow 063 -- Multi-File Batch Processing."""

import csv
import importlib.util
import json
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_063",
    Path(__file__).resolve().parent.parent / "flows" / "063_multi_file_batch.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_063"] = _mod
_spec.loader.exec_module(_mod)

UnifiedRecord = _mod.UnifiedRecord
BatchResult = _mod.BatchResult
discover_files = _mod.discover_files
read_file = _mod.read_file
harmonize_columns = _mod.harmonize_columns
compute_record_hash = _mod.compute_record_hash
deduplicate = _mod.deduplicate
summarize_batch = _mod.summarize_batch
multi_file_batch_flow = _mod.multi_file_batch_flow


def test_unified_record_model() -> None:
    record = UnifiedRecord(source_file="a.csv", format="csv", id="1", name="A", value=1.0, record_hash="abc")
    assert record.format == "csv"


def test_discover_files(tmp_path: Path) -> None:
    (tmp_path / "a.csv").write_text("x")
    (tmp_path / "b.json").write_text("{}")
    (tmp_path / "c.txt").write_text("x")
    result = discover_files.fn(str(tmp_path), [".csv", ".json"])
    assert len(result) == 2


def test_read_csv_file(tmp_path: Path) -> None:
    path = tmp_path / "data.csv"
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["id", "name"])
        w.writeheader()
        w.writerow({"id": "1", "name": "Alice"})
    result = read_file.fn(path)
    assert len(result) == 1
    assert result[0]["name"] == "Alice"


def test_read_json_file(tmp_path: Path) -> None:
    path = tmp_path / "data.json"
    path.write_text(json.dumps([{"id": "1", "name": "Bob"}]))
    result = read_file.fn(path)
    assert len(result) == 1
    assert result[0]["name"] == "Bob"


def test_harmonize_columns() -> None:
    records = [{"user_id": "1", "user_name": "Alice"}]
    result = harmonize_columns.fn(records, {"user_id": "id", "user_name": "name"})
    assert result[0]["id"] == "1"
    assert result[0]["name"] == "Alice"


def test_compute_record_hash() -> None:
    h1 = compute_record_hash.fn({"id": "1", "name": "A"}, ["id", "name"])
    h2 = compute_record_hash.fn({"id": "1", "name": "A"}, ["id", "name"])
    h3 = compute_record_hash.fn({"id": "2", "name": "B"}, ["id", "name"])
    assert h1 == h2
    assert h1 != h3


def test_deduplicate() -> None:
    records = [{"id": "1", "name": "A", "value": "10"}, {"id": "1", "name": "A", "value": "10"}]
    result = deduplicate.fn(records, ["a.csv", "b.json"], ["csv", "json"], ["id", "name"])
    assert len(result) == 1


def test_flow_runs(tmp_path: Path) -> None:
    state = multi_file_batch_flow(work_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
