"""Tests for flow 081 -- Air Quality Index."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "analytics_air_quality_index",
    Path(__file__).resolve().parent.parent.parent / "flows" / "analytics" / "analytics_air_quality_index.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["analytics_air_quality_index"] = _mod
_spec.loader.exec_module(_mod)

PollutantReading = _mod.PollutantReading
AqiClassification = _mod.AqiClassification
AirQualityReport = _mod.AirQualityReport
simulate_air_quality = _mod.simulate_air_quality
classify_aqi = _mod.classify_aqi
count_exceedances = _mod.count_exceedances
air_quality_summary = _mod.air_quality_summary
air_quality_index_flow = _mod.air_quality_index_flow
WHO_LIMITS = _mod.WHO_LIMITS


def test_simulate_air_quality() -> None:
    readings = simulate_air_quality.fn(["Oslo", "Bergen"])
    assert len(readings) == 8  # 2 cities * 4 pollutants
    assert all(isinstance(r, PollutantReading) for r in readings)
    assert all(len(r.hourly_values) == 24 for r in readings)


def test_simulate_air_quality_deterministic() -> None:
    r1 = simulate_air_quality.fn(["Oslo"])
    r2 = simulate_air_quality.fn(["Oslo"])
    assert r1[0].hourly_values == r2[0].hourly_values


def test_classify_aqi() -> None:
    readings = simulate_air_quality.fn(["Oslo"])
    classifications = classify_aqi.fn(readings)
    assert len(classifications) == 4
    assert all(isinstance(c, AqiClassification) for c in classifications)
    assert all(
        c.category in {"Good", "Moderate", "Unhealthy for Sensitive Groups", "Unhealthy", "Very Unhealthy", "Hazardous"}
        for c in classifications
    )


def test_classify_aqi_boundary() -> None:
    reading = PollutantReading(city="Test", pollutant="PM2.5", hourly_values=[50.0] * 24)
    classifications = classify_aqi.fn([reading])
    assert classifications[0].category == "Good"
    assert classifications[0].color == "green"


def test_classify_aqi_high_value() -> None:
    reading = PollutantReading(city="Test", pollutant="PM2.5", hourly_values=[250.0] * 24)
    classifications = classify_aqi.fn([reading])
    assert classifications[0].category == "Very Unhealthy"


def test_count_exceedances() -> None:
    readings = simulate_air_quality.fn(["Oslo", "Bergen", "Trondheim"])
    exceedances = count_exceedances.fn(readings, WHO_LIMITS)
    assert isinstance(exceedances, dict)
    assert all(v >= 0 for v in exceedances.values())


def test_air_quality_summary() -> None:
    readings = simulate_air_quality.fn(["Oslo", "Bergen"])
    classifications = classify_aqi.fn(readings)
    exceedances = count_exceedances.fn(readings, WHO_LIMITS)
    report = air_quality_summary.fn(classifications, exceedances, len(readings))
    assert isinstance(report, AirQualityReport)
    assert report.worst_city in {"Oslo", "Bergen"}
    assert report.total_readings == 8


def test_flow_runs() -> None:
    state = air_quality_index_flow(cities=["Oslo", "Bergen"], return_state=True)
    assert state.is_completed()
