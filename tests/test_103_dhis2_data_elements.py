"""Tests for flow 103 -- DHIS2 Data Elements API."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_103",
    Path(__file__).resolve().parent.parent / "flows" / "103_dhis2_data_elements.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_103"] = _mod
_spec.loader.exec_module(_mod)

Dhis2Connection = _mod.Dhis2Connection
RawDataElement = _mod.RawDataElement
FlatDataElement = _mod.FlatDataElement
DataElementReport = _mod.DataElementReport
fetch_data_elements = _mod.fetch_data_elements
flatten_data_elements = _mod.flatten_data_elements
write_data_element_csv = _mod.write_data_element_csv
data_element_report = _mod.data_element_report
dhis2_data_elements_flow = _mod.dhis2_data_elements_flow


def test_fetch_data_elements() -> None:
    conn = Dhis2Connection()
    elements = fetch_data_elements.fn(conn, "district")
    assert len(elements) == 15
    assert all(isinstance(e, RawDataElement) for e in elements)


def test_flatten_data_elements() -> None:
    conn = Dhis2Connection()
    raw = fetch_data_elements.fn(conn, "district")
    flat = flatten_data_elements.fn(raw)
    assert len(flat) == 15
    assert all(isinstance(e, FlatDataElement) for e in flat)


def test_category_combo_extraction() -> None:
    conn = Dhis2Connection()
    raw = fetch_data_elements.fn(conn, "district")
    flat = flatten_data_elements.fn(raw)
    with_cc = [e for e in flat if e.category_combo_id]
    without_cc = [e for e in flat if not e.category_combo_id]
    assert len(with_cc) > 0
    assert len(without_cc) > 0


def test_has_code_logic() -> None:
    conn = Dhis2Connection()
    raw = fetch_data_elements.fn(conn, "district")
    flat = flatten_data_elements.fn(raw)
    coded = [e for e in flat if e.has_code]
    uncoded = [e for e in flat if not e.has_code]
    assert len(coded) > 0
    assert len(uncoded) > 0


def test_name_length() -> None:
    conn = Dhis2Connection()
    raw = fetch_data_elements.fn(conn, "district")
    flat = flatten_data_elements.fn(raw)
    for e in flat:
        assert e.name_length == len(e.name)


def test_report_code_coverage() -> None:
    conn = Dhis2Connection()
    raw = fetch_data_elements.fn(conn, "district")
    flat = flatten_data_elements.fn(raw)
    report = data_element_report.fn(flat)
    assert 0.0 <= report.code_coverage <= 1.0
    assert report.total == 15


def test_write_csv(tmp_path: Path) -> None:
    conn = Dhis2Connection()
    raw = fetch_data_elements.fn(conn, "district")
    flat = flatten_data_elements.fn(raw)
    path = write_data_element_csv.fn(flat, str(tmp_path))
    assert path.exists()
    assert path.name == "data_elements.csv"


def test_flow_runs(tmp_path: Path) -> None:
    state = dhis2_data_elements_flow(output_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
