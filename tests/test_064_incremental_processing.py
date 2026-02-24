"""Tests for flow 064 -- Incremental Processing."""

import csv
import importlib.util
import json
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_064",
    Path(__file__).resolve().parent.parent / "flows" / "064_incremental_processing.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_064"] = _mod
_spec.loader.exec_module(_mod)

ProcessingManifest = _mod.ProcessingManifest
IncrementalResult = _mod.IncrementalResult
load_manifest = _mod.load_manifest
scan_directory = _mod.scan_directory
identify_new_files = _mod.identify_new_files
process_file = _mod.process_file
update_manifest = _mod.update_manifest
incremental_report = _mod.incremental_report
incremental_processing_flow = _mod.incremental_processing_flow


def _make_csv(path: Path, rows: int = 3) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "value"])
        writer.writeheader()
        for i in range(rows):
            writer.writerow({"id": str(i), "value": str(i * 10)})


def test_manifest_model() -> None:
    m = ProcessingManifest()
    assert m.total_processed == 0
    assert m.processed_files == {}


def test_load_manifest_missing(tmp_path: Path) -> None:
    manifest = load_manifest.fn(tmp_path / "missing.json")
    assert manifest.total_processed == 0


def test_load_manifest_existing(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(
        json.dumps({"last_updated": "2025-01-01", "processed_files": {"a.csv": "2025-01-01"}, "total_processed": 1})
    )
    manifest = load_manifest.fn(path)
    assert manifest.total_processed == 1


def test_scan_directory(tmp_path: Path) -> None:
    _make_csv(tmp_path / "a.csv")
    _make_csv(tmp_path / "b.csv")
    (tmp_path / "c.txt").write_text("x")
    files = scan_directory.fn(str(tmp_path), "*.csv")
    assert len(files) == 2


def test_identify_new_files(tmp_path: Path) -> None:
    files = [tmp_path / "a.csv", tmp_path / "b.csv"]
    manifest = ProcessingManifest(processed_files={"a.csv": "2025-01-01"})
    new = identify_new_files.fn(files, manifest)
    assert len(new) == 1
    assert new[0].name == "b.csv"


def test_process_file(tmp_path: Path) -> None:
    path = tmp_path / "test.csv"
    _make_csv(path, rows=4)
    result = process_file.fn(path)
    assert result["records"] == 4


def test_update_manifest(tmp_path: Path) -> None:
    manifest = ProcessingManifest()
    results = [{"filename": "a.csv", "records": 5}]
    updated = update_manifest.fn(manifest, results, tmp_path / "manifest.json")
    assert updated.total_processed == 1
    assert "a.csv" in updated.processed_files


def test_flow_idempotent(tmp_path: Path) -> None:
    """Run flow twice; second run should process zero new files."""
    result1 = incremental_processing_flow(work_dir=str(tmp_path))
    assert result1.new_files == 3

    result2 = incremental_processing_flow(work_dir=str(tmp_path))
    assert result2.new_files == 0
    assert result2.skipped_files == 3
