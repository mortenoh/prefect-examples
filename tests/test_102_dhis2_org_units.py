"""Tests for flow 102 -- DHIS2 Org Units API."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "flow_102",
    Path(__file__).resolve().parent.parent / "flows" / "102_dhis2_org_units.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_102"] = _mod
_spec.loader.exec_module(_mod)

Dhis2Connection = _mod.Dhis2Connection
FlatOrgUnit = _mod.FlatOrgUnit
OrgUnitReport = _mod.OrgUnitReport
fetch_org_units = _mod.fetch_org_units
flatten_org_units = _mod.flatten_org_units
write_org_unit_csv = _mod.write_org_unit_csv
org_unit_report = _mod.org_unit_report
dhis2_org_units_flow = _mod.dhis2_org_units_flow

# Sample data matching the real DHIS2 API shape
SAMPLE_ORG_UNITS = [
    {
        "id": "OU001",
        "name": "National",
        "shortName": "Nat",
        "level": 1,
        "parent": None,
        "path": "/OU001",
        "createdBy": {"username": "admin"},
        "translations": [],
        "openingDate": "2020-01-01",
    },
    {
        "id": "OU002",
        "name": "Region North",
        "shortName": "RN",
        "level": 2,
        "parent": {"id": "OU001"},
        "path": "/OU001/OU002",
        "createdBy": {"username": "admin"},
        "translations": [{"locale": "fr", "value": "Nord"}],
        "openingDate": "2020-03-01",
    },
    {
        "id": "OU003",
        "name": "District Alpha",
        "shortName": "DA",
        "level": 3,
        "parent": {"id": "OU002"},
        "path": "/OU001/OU002/OU003",
        "createdBy": None,
        "translations": [],
        "openingDate": "2020-06-01",
    },
]


def _mock_response(json_data: dict, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


@patch("httpx.get")
def test_fetch_org_units(mock_get: MagicMock) -> None:
    mock_get.return_value = _mock_response({"organisationUnits": SAMPLE_ORG_UNITS})
    conn = Dhis2Connection()
    units = fetch_org_units.fn(conn, "district")
    assert len(units) == 3


def test_flatten_org_units() -> None:
    flat = flatten_org_units.fn(SAMPLE_ORG_UNITS)
    assert len(flat) == 3
    assert all(isinstance(u, FlatOrgUnit) for u in flat)


def test_depth_from_path() -> None:
    flat = flatten_org_units.fn(SAMPLE_ORG_UNITS)
    national = next(u for u in flat if u.name == "National")
    district = next(u for u in flat if u.name == "District Alpha")
    assert national.hierarchy_depth == 0
    assert district.hierarchy_depth == 2


def test_parent_extraction() -> None:
    flat = flatten_org_units.fn(SAMPLE_ORG_UNITS)
    region = next(u for u in flat if u.name == "Region North")
    assert region.parent_id == "OU001"
    national = next(u for u in flat if u.name == "National")
    assert national.parent_id == ""


def test_translation_count() -> None:
    flat = flatten_org_units.fn(SAMPLE_ORG_UNITS)
    region = next(u for u in flat if u.name == "Region North")
    national = next(u for u in flat if u.name == "National")
    assert region.translation_count == 1
    assert national.translation_count == 0


def test_level_distribution() -> None:
    flat = flatten_org_units.fn(SAMPLE_ORG_UNITS)
    report = org_unit_report.fn(flat)
    assert report.total == 3
    assert sum(report.level_distribution.values()) == 3


def test_write_csv(tmp_path: Path) -> None:
    flat = flatten_org_units.fn(SAMPLE_ORG_UNITS)
    path = write_org_unit_csv.fn(flat, str(tmp_path))
    assert path.exists()
    assert path.name == "org_units.csv"


@patch("httpx.get")
def test_flow_runs(mock_get: MagicMock, tmp_path: Path) -> None:
    mock_get.return_value = _mock_response({"organisationUnits": SAMPLE_ORG_UNITS})
    state = dhis2_org_units_flow(output_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
