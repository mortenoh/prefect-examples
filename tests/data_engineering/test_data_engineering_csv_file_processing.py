"""Tests for flow 061 -- CSV File Processing."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "data_engineering_csv_file_processing",
    Path(__file__).resolve().parent.parent.parent
    / "flows"
    / "data_engineering"
    / "data_engineering_csv_file_processing.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["data_engineering_csv_file_processing"] = _mod
_spec.loader.exec_module(_mod)

CsvRecord = _mod.CsvRecord
CsvProcessingResult = _mod.CsvProcessingResult
generate_csv = _mod.generate_csv
read_csv = _mod.read_csv
validate_csv_row = _mod.validate_csv_row
transform_csv_rows = _mod.transform_csv_rows
write_csv = _mod.write_csv
archive_file = _mod.archive_file
csv_file_processing_flow = _mod.csv_file_processing_flow


def test_csv_record_model() -> None:
    record = CsvRecord(row_number=1, data={"id": "1", "name": "Alice"})
    assert record.valid is True
    assert record.errors == []


def test_generate_csv(tmp_path: Path) -> None:
    path = generate_csv.fn(str(tmp_path), "test.csv", rows=5)
    assert path.exists()
    lines = path.read_text().strip().split("\n")
    assert len(lines) == 6  # header + 5 rows


def test_read_csv(tmp_path: Path) -> None:
    path = generate_csv.fn(str(tmp_path), "test.csv", rows=3)
    rows = read_csv.fn(path)
    assert len(rows) == 3
    assert "id" in rows[0]


def test_validate_csv_row_valid() -> None:
    row = {"id": "1", "name": "Alice", "email": "a@b.com"}
    record = validate_csv_row.fn(row, 1, ["id", "name", "email"])
    assert record.valid is True


def test_validate_csv_row_invalid() -> None:
    row = {"id": "1", "name": "", "email": "a@b.com"}
    record = validate_csv_row.fn(row, 1, ["id", "name", "email"])
    assert record.valid is False
    assert len(record.errors) == 1


def test_transform_csv_rows() -> None:
    records = [
        CsvRecord(row_number=1, data={"name": "alice", "score": "50.0"}, valid=True),
        CsvRecord(row_number=2, data={"name": "bob", "score": "60.0"}, valid=False),
    ]
    result = transform_csv_rows.fn(records)
    assert len(result) == 1
    assert result[0]["name"] == "ALICE"


def test_write_and_read_csv(tmp_path: Path) -> None:
    rows = [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]
    path = write_csv.fn(str(tmp_path), "out.csv", rows)
    assert path.exists()
    read_back = read_csv.fn(path)
    assert len(read_back) == 2


def test_flow_runs(tmp_path: Path) -> None:
    state = csv_file_processing_flow(work_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
