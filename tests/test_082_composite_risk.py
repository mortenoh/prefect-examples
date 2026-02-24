"""Tests for flow 082 -- Composite Risk Assessment."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_082",
    Path(__file__).resolve().parent.parent / "flows" / "082_composite_risk.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_082"] = _mod
_spec.loader.exec_module(_mod)

RiskFactor = _mod.RiskFactor
CompositeRisk = _mod.CompositeRisk
RiskReport = _mod.RiskReport
simulate_marine_data = _mod.simulate_marine_data
simulate_flood_data = _mod.simulate_flood_data
normalize_risk = _mod.normalize_risk
compute_composite = _mod.compute_composite
risk_summary = _mod.risk_summary
composite_risk_flow = _mod.composite_risk_flow
RISK_THRESHOLDS = _mod.RISK_THRESHOLDS


def test_simulate_marine_data() -> None:
    factors = simulate_marine_data.fn(["A", "B"])
    assert len(factors) == 4  # 2 locations * 2 factors
    assert all(isinstance(f, RiskFactor) for f in factors)


def test_simulate_flood_data() -> None:
    factors = simulate_flood_data.fn(["A", "B"])
    assert len(factors) == 2
    assert all(f.source == "flood_discharge" for f in factors)


def test_normalize_risk() -> None:
    factors = [RiskFactor(source="marine_wave", location="A", raw_value=2.75)]
    normalized = normalize_risk.fn(factors, RISK_THRESHOLDS)
    assert 0.0 <= normalized[0].normalized_score <= 100.0


def test_normalize_risk_boundaries() -> None:
    low = RiskFactor(source="marine_wave", location="A", raw_value=0.0)
    high = RiskFactor(source="marine_wave", location="A", raw_value=10.0)
    results = normalize_risk.fn([low, high], RISK_THRESHOLDS)
    assert results[0].normalized_score == 0.0
    assert results[1].normalized_score == 100.0


def test_compute_composite() -> None:
    marine = [RiskFactor(source="marine_wave", location="A", raw_value=3.0, normalized_score=50.0)]
    flood = [RiskFactor(source="flood_discharge", location="A", raw_value=1.0, normalized_score=30.0)]
    result = compute_composite.fn("A", marine, flood, 0.6, 0.4)
    assert isinstance(result, CompositeRisk)
    expected = 50.0 * 0.6 + 30.0 * 0.4
    assert abs(result.weighted_score - expected) < 0.2


def test_classify_risk_categories() -> None:
    categories = set()
    for score in [10.0, 30.0, 60.0, 80.0]:
        marine = [RiskFactor(source="m", location="X", raw_value=0, normalized_score=score)]
        flood = [RiskFactor(source="f", location="X", raw_value=0, normalized_score=score)]
        result = compute_composite.fn("X", marine, flood, 0.5, 0.5)
        categories.add(result.category)
    assert len(categories) >= 3


def test_flow_runs() -> None:
    state = composite_risk_flow(locations=["A", "B"], return_state=True)
    assert state.is_completed()
