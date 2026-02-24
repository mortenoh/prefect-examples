"""Tests for flow 085 -- Demographic Analysis."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "analytics_demographic_analysis",
    Path(__file__).resolve().parent.parent.parent / "flows" / "analytics" / "analytics_demographic_analysis.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["analytics_demographic_analysis"] = _mod
_spec.loader.exec_module(_mod)

RawCountryData = _mod.RawCountryData
Country = _mod.Country
BridgeRecord = _mod.BridgeRecord
DemographicReport = _mod.DemographicReport
simulate_countries = _mod.simulate_countries
flatten_to_country_table = _mod.flatten_to_country_table
build_bridge_table = _mod.build_bridge_table
build_border_edges = _mod.build_border_edges
rank_countries = _mod.rank_countries
demographic_analysis_flow = _mod.demographic_analysis_flow


def test_simulate_countries() -> None:
    data = simulate_countries.fn()
    assert len(data) == 5
    assert all(isinstance(d, RawCountryData) for d in data)
    assert all(d.name for d in data)
    assert all(d.languages for d in data)


def test_flatten_to_country_table() -> None:
    raw = simulate_countries.fn()
    countries = flatten_to_country_table.fn(raw)
    assert len(countries) == 5
    assert all(isinstance(c, Country) for c in countries)
    assert all(c.density > 0 for c in countries)


def test_build_language_bridge() -> None:
    raw = simulate_countries.fn()
    countries = flatten_to_country_table.fn(raw)
    bridge = build_bridge_table.fn(countries, "languages")
    assert len(bridge) > 5  # Multiple languages across countries
    assert all(isinstance(b, BridgeRecord) for b in bridge)


def test_build_currency_bridge() -> None:
    raw = simulate_countries.fn()
    countries = flatten_to_country_table.fn(raw)
    bridge = build_bridge_table.fn(countries, "currencies")
    assert len(bridge) == 5  # One currency per country


def test_build_border_edges() -> None:
    raw = simulate_countries.fn()
    countries = flatten_to_country_table.fn(raw)
    edges = build_border_edges.fn(countries)
    assert len(edges) > 0
    # Check that if (A, B) exists and B has borders, (B, A) should also exist
    norway_sweden = ("Norway", "Sweden") in edges
    sweden_norway = ("Sweden", "Norway") in edges
    assert norway_sweden and sweden_norway


def test_border_edges_iceland_isolated() -> None:
    raw = simulate_countries.fn()
    countries = flatten_to_country_table.fn(raw)
    edges = build_border_edges.fn(countries)
    iceland_edges = [e for e in edges if "Iceland" in e]
    assert len(iceland_edges) == 0


def test_rank_countries() -> None:
    raw = simulate_countries.fn()
    countries = flatten_to_country_table.fn(raw)
    ranking = rank_countries.fn(countries, "population")
    assert ranking[0] == "Sweden"  # Highest population
    assert ranking[-1] == "Iceland"  # Lowest population


def test_flow_runs() -> None:
    state = demographic_analysis_flow(return_state=True)
    assert state.is_completed()
