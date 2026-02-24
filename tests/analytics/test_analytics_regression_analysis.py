"""Tests for flow 089 -- Regression Analysis."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "analytics_regression_analysis",
    Path(__file__).resolve().parent.parent.parent / "flows" / "analytics" / "analytics_regression_analysis.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["analytics_regression_analysis"] = _mod
_spec.loader.exec_module(_mod)

RegressionResult = _mod.RegressionResult
EfficiencyRank = _mod.EfficiencyRank
RegressionReport = _mod.RegressionReport
HealthRecord = _mod.HealthRecord
simulate_health_data = _mod.simulate_health_data
log_transform = _mod.log_transform
linear_regression = _mod.linear_regression
predict = _mod.predict
rank_by_residual = _mod.rank_by_residual
regression_analysis_flow = _mod.regression_analysis_flow


def test_simulate_health_data() -> None:
    data = simulate_health_data.fn(["A", "B", "C"])
    assert len(data) == 3
    assert all(isinstance(d, HealthRecord) for d in data)
    assert all(d.spending > 0 for d in data)


def test_log_transform() -> None:
    import math

    values = [1.0, 10.0, 100.0]
    result = log_transform.fn(values)
    assert len(result) == 3
    assert abs(result[0] - math.log(1.0)) < 0.001
    assert abs(result[1] - math.log(10.0)) < 0.001


def test_linear_regression_perfect() -> None:
    x = [1.0, 2.0, 3.0, 4.0, 5.0]
    y = [2.0, 4.0, 6.0, 8.0, 10.0]
    result = linear_regression.fn(x, y)
    assert abs(result.slope - 2.0) < 0.01
    assert abs(result.intercept - 0.0) < 0.01
    assert abs(result.r_squared - 1.0) < 0.01


def test_r_squared_range() -> None:
    countries = ["A", "B", "C", "D", "E", "F", "G", "H"]
    data = simulate_health_data.fn(countries)
    spending = [d.spending for d in data]
    mortality = [d.mortality for d in data]
    log_s = log_transform.fn(spending)
    result = linear_regression.fn(log_s, mortality)
    assert 0.0 <= result.r_squared <= 1.0


def test_predict() -> None:
    reg = RegressionResult(slope=2.0, intercept=1.0, r_squared=1.0, n=5)
    predictions = predict.fn(reg, [1.0, 2.0, 3.0])
    assert predictions == [3.0, 5.0, 7.0]


def test_rank_by_residual() -> None:
    actuals = [10.0, 20.0, 15.0]
    predicted = [12.0, 18.0, 16.0]
    entities = ["A", "B", "C"]
    rankings = rank_by_residual.fn(actuals, predicted, entities)
    assert rankings[0].rank == 1
    assert rankings[-1].rank == 3
    # Most efficient = most negative residual
    assert rankings[0].residual <= rankings[-1].residual


def test_residual_sum_near_zero() -> None:
    countries = ["A", "B", "C", "D", "E"]
    data = simulate_health_data.fn(countries)
    spending = [d.spending for d in data]
    mortality = [d.mortality for d in data]
    entities = [d.country for d in data]
    log_s = log_transform.fn(spending)
    reg = linear_regression.fn(log_s, mortality)
    pred = predict.fn(reg, log_s)
    rankings = rank_by_residual.fn(mortality, pred, entities)
    residual_sum = sum(r.residual for r in rankings)
    assert abs(residual_sum) < 1.0  # Residuals should sum near zero


def test_flow_runs() -> None:
    state = regression_analysis_flow(countries=["A", "B", "C", "D", "E"], return_state=True)
    assert state.is_completed()
