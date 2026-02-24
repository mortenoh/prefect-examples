"""Tests for flow 088 -- Hypothesis Testing."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_088",
    Path(__file__).resolve().parent.parent / "flows" / "088_hypothesis_testing.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_088"] = _mod
_spec.loader.exec_module(_mod)

SeismicRecord = _mod.SeismicRecord
WeatherRecord = _mod.WeatherRecord
DailyObservation = _mod.DailyObservation
HypothesisResult = _mod.HypothesisResult
HypothesisReport = _mod.HypothesisReport
simulate_seismic_data = _mod.simulate_seismic_data
simulate_weather_data = _mod.simulate_weather_data
align_datasets = _mod.align_datasets
check_correlation = _mod.check_correlation
hypothesis_summary = _mod.hypothesis_summary
hypothesis_testing_flow = _mod.hypothesis_testing_flow


def test_simulate_seismic_data() -> None:
    data = simulate_seismic_data.fn(30)
    assert len(data) == 30
    assert all(isinstance(d, SeismicRecord) for d in data)
    assert all(0 <= d.quake_count < 5 for d in data)


def test_simulate_seismic_deterministic() -> None:
    d1 = simulate_seismic_data.fn(10)
    d2 = simulate_seismic_data.fn(10)
    assert d1 == d2


def test_simulate_weather_data() -> None:
    data = simulate_weather_data.fn(30)
    assert len(data) == 30
    assert all(isinstance(d, WeatherRecord) for d in data)


def test_align_datasets() -> None:
    seismic = simulate_seismic_data.fn(30)
    weather = simulate_weather_data.fn(30)
    aligned = align_datasets.fn(seismic, weather, "date")
    assert len(aligned) == 30
    assert all(isinstance(o, DailyObservation) for o in aligned)


def test_correlation_near_zero() -> None:
    seismic = simulate_seismic_data.fn(365)
    weather = simulate_weather_data.fn(365)
    aligned = align_datasets.fn(seismic, weather, "date")
    result = check_correlation.fn(aligned, "quakes vs temp")
    assert abs(result.r_value) < 0.3  # Expect weak/no correlation


def test_check_correlation_insufficient_data() -> None:
    aligned = [DailyObservation(date="d1", metric_a=1.0, metric_b=2.0)]
    result = check_correlation.fn(aligned, "test")
    assert result.interpretation == "Insufficient data"
    assert not result.is_significant


def test_flow_runs() -> None:
    state = hypothesis_testing_flow(days=60, return_state=True)
    assert state.is_completed()
