"""Tests for flow 086 -- Multi-Indicator Correlation."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_086",
    Path(__file__).resolve().parent.parent / "flows" / "086_multi_indicator_correlation.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_086"] = _mod
_spec.loader.exec_module(_mod)

IndicatorSeries = _mod.IndicatorSeries
JoinedRecord = _mod.JoinedRecord
GrowthRates = _mod.GrowthRates
CorrelationResult = _mod.CorrelationResult
IndicatorReport = _mod.IndicatorReport
simulate_indicator = _mod.simulate_indicator
join_indicators = _mod.join_indicators
forward_fill = _mod.forward_fill
compute_correlations = _mod.compute_correlations
compute_growth_rates = _mod.compute_growth_rates
multi_indicator_correlation_flow = _mod.multi_indicator_correlation_flow


def test_simulate_indicator() -> None:
    series = simulate_indicator.fn("GDP", ["A", "B"], [2020, 2021, 2022])
    assert len(series) == 2
    assert all(isinstance(s, IndicatorSeries) for s in series)
    assert all(len(s.year_values) == 3 for s in series)


def test_join_indicators() -> None:
    countries = ["A", "B"]
    years = [2020, 2021]
    gdp = simulate_indicator.fn("GDP", countries, years)
    co2 = simulate_indicator.fn("CO2", countries, years)
    joined = join_indicators.fn([gdp, co2], countries, years)
    assert len(joined) == 4  # 2 countries * 2 years
    assert all(isinstance(r, JoinedRecord) for r in joined)
    assert all(r.GDP is not None and r.CO2 is not None for r in joined)


def test_forward_fill() -> None:
    records = [
        JoinedRecord(country="A", year=2020, GDP=100.0),
        JoinedRecord(country="A", year=2021, GDP=None),
        JoinedRecord(country="A", year=2022, GDP=120.0),
    ]
    filled = forward_fill.fn(records, ["GDP"])
    assert filled[1].GDP == 100.0  # Forward-filled from 2020


def test_forward_fill_no_prior() -> None:
    records = [
        JoinedRecord(country="A", year=2020, GDP=None),
        JoinedRecord(country="A", year=2021, GDP=100.0),
    ]
    filled = forward_fill.fn(records, ["GDP"])
    assert filled[0].GDP is None  # No prior value to fill from


def test_compute_correlations() -> None:
    records = [JoinedRecord(country="A", year=2020 + i, GDP=float(i), CO2=float(i * 2)) for i in range(10)]
    corrs = compute_correlations.fn(records, ["GDP", "CO2"])
    assert len(corrs) == 1
    assert -1.0 <= corrs[0].r_value <= 1.0
    assert corrs[0].r_value > 0.99  # Perfect positive correlation


def test_compute_growth_rates() -> None:
    records = [
        JoinedRecord(country="A", year=2020, GDP=100.0),
        JoinedRecord(country="A", year=2021, GDP=110.0),
        JoinedRecord(country="A", year=2022, GDP=121.0),
    ]
    growth = compute_growth_rates.fn(records, "GDP")
    assert isinstance(growth, GrowthRates)
    assert "A" in growth.rates
    assert abs(growth.rates["A"] - 0.1) < 0.01  # ~10% growth per year


def test_correlation_range() -> None:
    countries = ["A", "B", "C"]
    years = list(range(2010, 2021))
    gdp = simulate_indicator.fn("GDP", countries, years)
    co2 = simulate_indicator.fn("CO2", countries, years)
    ren = simulate_indicator.fn("Renewable", countries, years)
    joined = join_indicators.fn([gdp, co2, ren], countries, years)
    filled = forward_fill.fn(joined, ["GDP", "CO2", "Renewable"])
    corrs = compute_correlations.fn(filled, ["GDP", "CO2", "Renewable"])
    for c in corrs:
        assert -1.0 <= c.r_value <= 1.0


def test_flow_runs() -> None:
    state = multi_indicator_correlation_flow(
        countries=["A", "B"],
        years=[2020, 2021, 2022],
        return_state=True,
    )
    assert state.is_completed()
