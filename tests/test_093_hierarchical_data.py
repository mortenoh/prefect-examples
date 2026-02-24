"""Tests for flow 093 -- Hierarchical Data Processing."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_093",
    Path(__file__).resolve().parent.parent / "flows" / "093_hierarchical_data.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_093"] = _mod
_spec.loader.exec_module(_mod)

ParentRef = _mod.ParentRef
RawOrgUnit = _mod.RawOrgUnit
OrgUnit = _mod.OrgUnit
RootsAndLeaves = _mod.RootsAndLeaves
HierarchyReport = _mod.HierarchyReport
simulate_org_hierarchy = _mod.simulate_org_hierarchy
flatten_hierarchy = _mod.flatten_hierarchy
compute_level_distribution = _mod.compute_level_distribution
find_roots_and_leaves = _mod.find_roots_and_leaves
hierarchy_summary = _mod.hierarchy_summary
hierarchical_data_flow = _mod.hierarchical_data_flow


def test_simulate_org_hierarchy() -> None:
    data = simulate_org_hierarchy.fn()
    assert len(data) == 8
    assert all(isinstance(d, RawOrgUnit) for d in data)


def test_flatten_hierarchy() -> None:
    raw = simulate_org_hierarchy.fn()
    units = flatten_hierarchy.fn(raw)
    assert len(units) == 8
    assert all(isinstance(u, OrgUnit) for u in units)


def test_depth_from_path() -> None:
    raw = simulate_org_hierarchy.fn()
    units = flatten_hierarchy.fn(raw)
    national = next(u for u in units if u.name == "National")
    facility = next(u for u in units if u.name == "Facility One")
    assert national.hierarchy_depth == 1
    assert facility.hierarchy_depth == 4


def test_parent_extraction() -> None:
    raw = simulate_org_hierarchy.fn()
    units = flatten_hierarchy.fn(raw)
    region = next(u for u in units if u.name == "Region North")
    assert region.parent_id == "OU_001"
    assert region.parent_name == "National"


def test_root_has_no_parent() -> None:
    raw = simulate_org_hierarchy.fn()
    units = flatten_hierarchy.fn(raw)
    root = next(u for u in units if u.name == "National")
    assert root.parent_id == ""
    assert root.parent_name == ""


def test_level_distribution() -> None:
    raw = simulate_org_hierarchy.fn()
    units = flatten_hierarchy.fn(raw)
    dist = compute_level_distribution.fn(units)
    assert dist[1] == 1  # National
    assert dist[2] == 2  # 2 regions
    assert dist[3] == 3  # 3 districts
    assert dist[4] == 2  # 2 facilities


def test_flow_runs() -> None:
    state = hierarchical_data_flow(return_state=True)
    assert state.is_completed()
