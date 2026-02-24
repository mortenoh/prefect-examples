"""Tests for flow 084 -- Statistical Aggregation."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_084",
    Path(__file__).resolve().parent.parent / "flows" / "084_statistical_aggregation.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_084"] = _mod
_spec.loader.exec_module(_mod)

WeatherObservation = _mod.WeatherObservation
GroupStats = _mod.GroupStats
AggregationResult = _mod.AggregationResult
CrossTab = _mod.CrossTab
AggregationReport = _mod.AggregationReport
generate_weather_data = _mod.generate_weather_data
aggregate_by_group = _mod.aggregate_by_group
build_cross_tab = _mod.build_cross_tab
aggregation_summary = _mod.aggregation_summary
statistical_aggregation_flow = _mod.statistical_aggregation_flow


def test_generate_weather_data() -> None:
    records = generate_weather_data.fn(["A", "B"], 3)
    assert len(records) == 6  # 2 stations * 3 days
    assert all(isinstance(r, WeatherObservation) for r in records)


def test_generate_weather_data_deterministic() -> None:
    r1 = generate_weather_data.fn(["A"], 5)
    r2 = generate_weather_data.fn(["A"], 5)
    assert [r.temperature for r in r1] == [r.temperature for r in r2]


def test_aggregate_by_group() -> None:
    records = generate_weather_data.fn(["A", "B"], 5)
    result = aggregate_by_group.fn(records, "station", "temperature")
    assert isinstance(result, AggregationResult)
    assert result.metric_count == 2
    assert all(g.count == 5 for g in result.groups)


def test_aggregate_min_max() -> None:
    records = [
        WeatherObservation(station="x", day="d1", temperature=10.0, humidity=50.0),
        WeatherObservation(station="x", day="d2", temperature=20.0, humidity=50.0),
        WeatherObservation(station="x", day="d3", temperature=30.0, humidity=50.0),
    ]
    result = aggregate_by_group.fn(records, "station", "temperature")
    g = result.groups[0]
    assert g.min_val == 10.0
    assert g.max_val == 30.0
    assert g.mean == 20.0


def test_build_cross_tab() -> None:
    records = generate_weather_data.fn(["A", "B"], 3)
    ct = build_cross_tab.fn(records, "station", "day", "temperature")
    assert isinstance(ct, CrossTab)
    assert len(ct.row_labels) == 2
    assert len(ct.col_labels) == 3
    assert len(ct.matrix) == 2
    assert len(ct.matrix[0]) == 3


def test_aggregation_summary() -> None:
    records = generate_weather_data.fn(["A", "B"], 3)
    sta = aggregate_by_group.fn(records, "station", "temperature")
    dat = aggregate_by_group.fn(records, "day", "temperature")
    ct = build_cross_tab.fn(records, "station", "day", "temperature")
    report = aggregation_summary.fn(sta, dat, ct, len(records))
    assert isinstance(report, AggregationReport)
    assert report.record_count == 6


def test_cross_tab_values_in_range() -> None:
    records = generate_weather_data.fn(["A"], 5)
    ct = build_cross_tab.fn(records, "station", "day", "temperature")
    temps = [r.temperature for r in records]
    for row in ct.matrix:
        for val in row:
            assert min(temps) <= val <= max(temps)


def test_flow_runs() -> None:
    state = statistical_aggregation_flow(stations=["A", "B"], days=3, return_state=True)
    assert state.is_completed()
