"""Tests for flow 102 -- DHIS2 Org Units API."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_102",
    Path(__file__).resolve().parent.parent / "flows" / "102_dhis2_org_units.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_102"] = _mod
_spec.loader.exec_module(_mod)

Dhis2Connection = _mod.Dhis2Connection
RawOrgUnit = _mod.RawOrgUnit
FlatOrgUnit = _mod.FlatOrgUnit
OrgUnitReport = _mod.OrgUnitReport
fetch_org_units = _mod.fetch_org_units
flatten_org_units = _mod.flatten_org_units
write_org_unit_csv = _mod.write_org_unit_csv
org_unit_report = _mod.org_unit_report
dhis2_org_units_flow = _mod.dhis2_org_units_flow


def test_fetch_org_units() -> None:
    conn = Dhis2Connection()
    units = fetch_org_units.fn(conn, "district")
    assert len(units) == 20
    assert all(isinstance(u, RawOrgUnit) for u in units)


def test_flatten_org_units() -> None:
    conn = Dhis2Connection()
    raw = fetch_org_units.fn(conn, "district")
    flat = flatten_org_units.fn(raw)
    assert len(flat) == 20
    assert all(isinstance(u, FlatOrgUnit) for u in flat)


def test_depth_from_path() -> None:
    conn = Dhis2Connection()
    raw = fetch_org_units.fn(conn, "district")
    flat = flatten_org_units.fn(raw)
    for u in flat:
        assert u.hierarchy_depth >= 1


def test_parent_extraction() -> None:
    conn = Dhis2Connection()
    raw = fetch_org_units.fn(conn, "district")
    flat = flatten_org_units.fn(raw)
    level1_units = [u for u in flat if u.level == 1]
    for u in level1_units:
        assert u.parent_id == ""


def test_translation_count() -> None:
    conn = Dhis2Connection()
    raw = fetch_org_units.fn(conn, "district")
    flat = flatten_org_units.fn(raw)
    translated = [u for u in flat if u.translation_count > 0]
    assert len(translated) > 0


def test_level_distribution() -> None:
    conn = Dhis2Connection()
    raw = fetch_org_units.fn(conn, "district")
    flat = flatten_org_units.fn(raw)
    report = org_unit_report.fn(flat)
    assert report.total == 20
    assert sum(report.level_distribution.values()) == 20


def test_write_csv(tmp_path: Path) -> None:
    conn = Dhis2Connection()
    raw = fetch_org_units.fn(conn, "district")
    flat = flatten_org_units.fn(raw)
    path = write_org_unit_csv.fn(flat, str(tmp_path))
    assert path.exists()
    assert path.name == "org_units.csv"


def test_flow_runs(tmp_path: Path) -> None:
    state = dhis2_org_units_flow(output_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
