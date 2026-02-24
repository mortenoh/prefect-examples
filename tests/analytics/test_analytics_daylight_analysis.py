"""Tests for flow 083 -- Daylight Analysis."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "analytics_daylight_analysis",
    Path(__file__).resolve().parent.parent.parent / "flows" / "analytics" / "analytics_daylight_analysis.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["analytics_daylight_analysis"] = _mod
_spec.loader.exec_module(_mod)

DaylightRecord = _mod.DaylightRecord
SeasonalProfile = _mod.SeasonalProfile
DaylightReport = _mod.DaylightReport
simulate_daylight = _mod.simulate_daylight
compute_seasonal_profiles = _mod.compute_seasonal_profiles
correlate_latitude_amplitude = _mod.correlate_latitude_amplitude
daylight_summary = _mod.daylight_summary
daylight_analysis_flow = _mod.daylight_analysis_flow


def test_simulate_daylight() -> None:
    records = simulate_daylight.fn([("Oslo", 59.9), ("Nairobi", -1.3)])
    assert len(records) == 24  # 2 cities * 12 months
    assert all(isinstance(r, DaylightRecord) for r in records)


def test_simulate_daylight_deterministic() -> None:
    r1 = simulate_daylight.fn([("Oslo", 59.9)])
    r2 = simulate_daylight.fn([("Oslo", 59.9)])
    assert [r.day_length_hours for r in r1] == [r.day_length_hours for r in r2]


def test_day_length_varies_with_latitude() -> None:
    equator = simulate_daylight.fn([("Equator", 0.0)])
    arctic = simulate_daylight.fn([("Arctic", 65.0)])
    equator_range = max(r.day_length_hours for r in equator) - min(r.day_length_hours for r in equator)
    arctic_range = max(r.day_length_hours for r in arctic) - min(r.day_length_hours for r in arctic)
    assert arctic_range > equator_range


def test_compute_seasonal_profiles() -> None:
    records = simulate_daylight.fn([("Oslo", 59.9), ("Nairobi", -1.3)])
    profiles = compute_seasonal_profiles.fn(records)
    assert len(profiles) == 2
    oslo = next(p for p in profiles if p.city == "Oslo")
    nairobi = next(p for p in profiles if p.city == "Nairobi")
    assert oslo.amplitude > nairobi.amplitude


def test_correlation_positive() -> None:
    cities = [("Equator", 0.0), ("Mid", 45.0), ("Arctic", 70.0)]
    records = simulate_daylight.fn(cities)
    profiles = compute_seasonal_profiles.fn(records)
    r = correlate_latitude_amplitude.fn(profiles)
    assert r > 0.9  # strong positive correlation


def test_correlation_single_city() -> None:
    records = simulate_daylight.fn([("Oslo", 59.9)])
    profiles = compute_seasonal_profiles.fn(records)
    r = correlate_latitude_amplitude.fn(profiles)
    assert r == 0.0


def test_flow_runs() -> None:
    cities = [("Oslo", 59.9), ("Nairobi", -1.3)]
    state = daylight_analysis_flow(cities_with_lat=cities, return_state=True)
    assert state.is_completed()
