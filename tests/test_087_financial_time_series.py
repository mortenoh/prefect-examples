"""Tests for flow 087 -- Financial Time Series."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_087",
    Path(__file__).resolve().parent.parent / "flows" / "087_financial_time_series.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_087"] = _mod
_spec.loader.exec_module(_mod)

LogReturn = _mod.LogReturn
RateRecord = _mod.RateRecord
VolatilityResult = _mod.VolatilityResult
MarketEvent = _mod.MarketEvent
TimeSeriesReport = _mod.TimeSeriesReport
simulate_exchange_rates = _mod.simulate_exchange_rates
compute_log_returns = _mod.compute_log_returns
rolling_volatility = _mod.rolling_volatility
correlation_matrix = _mod.correlation_matrix
detect_anomalies = _mod.detect_anomalies
financial_time_series_flow = _mod.financial_time_series_flow


def test_simulate_exchange_rates() -> None:
    rates = simulate_exchange_rates.fn(["EUR", "GBP"], 30)
    assert len(rates) == 60  # 2 currencies * 30 days
    assert all(isinstance(r, RateRecord) for r in rates)
    assert all(r.rate > 0 for r in rates)


def test_simulate_deterministic() -> None:
    r1 = simulate_exchange_rates.fn(["EUR"], 10)
    r2 = simulate_exchange_rates.fn(["EUR"], 10)
    assert [r.rate for r in r1] == [r.rate for r in r2]


def test_compute_log_returns() -> None:
    rates = simulate_exchange_rates.fn(["EUR"], 30)
    returns = compute_log_returns.fn(rates)
    assert "EUR" in returns
    assert len(returns["EUR"]) == 29  # n-1 returns from n rates
    assert all(isinstance(r, LogReturn) for r in returns["EUR"])


def test_rolling_volatility() -> None:
    import math

    rates = simulate_exchange_rates.fn(["EUR", "GBP"], 50)
    returns = compute_log_returns.fn(rates)
    vols = rolling_volatility.fn(returns, window=20, annualize_factor=math.sqrt(252))
    assert len(vols) == 2
    assert all(isinstance(v, VolatilityResult) for v in vols)
    assert all(v.annualized_vol >= 0 for v in vols)


def test_correlation_diagonal() -> None:
    rates = simulate_exchange_rates.fn(["EUR", "GBP", "JPY"], 50)
    returns = compute_log_returns.fn(rates)
    corrs = correlation_matrix.fn(returns)
    assert corrs["EUR:EUR"] == 1.0
    assert corrs["GBP:GBP"] == 1.0
    assert corrs["JPY:JPY"] == 1.0


def test_correlation_range() -> None:
    rates = simulate_exchange_rates.fn(["EUR", "GBP"], 50)
    returns = compute_log_returns.fn(rates)
    corrs = correlation_matrix.fn(returns)
    for val in corrs.values():
        assert -1.0 <= val <= 1.0


def test_detect_anomalies() -> None:
    rates = simulate_exchange_rates.fn(["EUR"], 100)
    returns = compute_log_returns.fn(rates)
    events = detect_anomalies.fn(returns, threshold_stdev=2.0)
    assert isinstance(events, list)
    for e in events:
        assert isinstance(e, MarketEvent)
        assert abs(e.z_score) > 2.0


def test_flow_runs() -> None:
    state = financial_time_series_flow(currencies=["EUR", "GBP"], days=50, return_state=True)
    assert state.is_completed()
