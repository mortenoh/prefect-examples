"""Tests for flow 065 -- Quality Rules Engine."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_065",
    Path(__file__).resolve().parent.parent / "flows" / "065_quality_rules_engine.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_065"] = _mod
_spec.loader.exec_module(_mod)

QualityRule = _mod.QualityRule
RuleResult = _mod.RuleResult
QualityReport = _mod.QualityReport
build_rules = _mod.build_rules
generate_sample_data = _mod.generate_sample_data
run_not_null_check = _mod.run_not_null_check
run_range_check = _mod.run_range_check
run_uniqueness_check = _mod.run_uniqueness_check
run_completeness_check = _mod.run_completeness_check
execute_rule = _mod.execute_rule
compute_quality_score = _mod.compute_quality_score
quality_rules_engine_flow = _mod.quality_rules_engine_flow


def test_quality_rule_model() -> None:
    rule = QualityRule(name="test", rule_type="not_null", column="id")
    assert rule.name == "test"


def test_build_rules() -> None:
    config = [{"name": "r1", "rule_type": "not_null", "column": "id"}]
    rules = build_rules.fn(config)
    assert len(rules) == 1
    assert isinstance(rules[0], QualityRule)


def test_not_null_check_pass() -> None:
    data = [{"name": "Alice"}, {"name": "Bob"}]
    result = run_not_null_check.fn(data, "name")
    assert result.passed is True
    assert result.score == 1.0


def test_not_null_check_fail() -> None:
    data = [{"name": "Alice"}, {"name": ""}]
    result = run_not_null_check.fn(data, "name")
    assert result.passed is False


def test_range_check_pass() -> None:
    data = [{"value": 10}, {"value": 50}]
    result = run_range_check.fn(data, "value", 0, 100)
    assert result.passed is True


def test_range_check_fail() -> None:
    data = [{"value": 10}, {"value": -5}]
    result = run_range_check.fn(data, "value", 0, 100)
    assert result.passed is False


def test_uniqueness_check() -> None:
    data = [{"id": 1}, {"id": 2}, {"id": 1}]
    result = run_uniqueness_check.fn(data, "id")
    assert result.passed is False


def test_completeness_check() -> None:
    data = [{"a": 1}, {"a": 2}]
    result = run_completeness_check.fn(data, 2)
    assert result.passed is True
    result2 = run_completeness_check.fn(data, 5)
    assert result2.passed is False


def test_compute_quality_score_green() -> None:
    results = [RuleResult(rule_name="r1", rule_type="t", passed=True, score=1.0)]
    report = compute_quality_score.fn(results)
    assert report.traffic_light == "green"


def test_compute_quality_score_red() -> None:
    results = [RuleResult(rule_name="r1", rule_type="t", passed=False, score=0.3)]
    report = compute_quality_score.fn(results)
    assert report.traffic_light == "red"


def test_flow_runs() -> None:
    state = quality_rules_engine_flow(return_state=True)
    assert state.is_completed()
