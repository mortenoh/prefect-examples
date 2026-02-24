"""Tests for flow 094 -- Expression Complexity Scoring."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "analytics_expression_scoring",
    Path(__file__).resolve().parent.parent.parent / "flows" / "analytics" / "analytics_expression_scoring.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["analytics_expression_scoring"] = _mod
_spec.loader.exec_module(_mod)

Expression = _mod.Expression
ComplexityScore = _mod.ComplexityScore
ScoringReport = _mod.ScoringReport
simulate_expressions = _mod.simulate_expressions
parse_operands = _mod.parse_operands
count_operators = _mod.count_operators
score_complexity = _mod.score_complexity
categorize_by_domain = _mod.categorize_by_domain
scoring_summary = _mod.scoring_summary
expression_scoring_flow = _mod.expression_scoring_flow


def test_simulate_expressions() -> None:
    exprs = simulate_expressions.fn()
    assert len(exprs) == 7
    assert all(isinstance(e, Expression) for e in exprs)


def test_parse_operands_single() -> None:
    count = parse_operands.fn("#{value}")
    assert count == 1


def test_parse_operands_multiple() -> None:
    count = parse_operands.fn("#{a} + #{b} - #{c}")
    assert count == 3


def test_parse_operands_none() -> None:
    count = parse_operands.fn("100 * 2")
    assert count == 0


def test_count_operators() -> None:
    count = count_operators.fn("#{a} + #{b} - #{c} * 100")
    assert count == 3


def test_score_complexity_bins() -> None:
    exprs = simulate_expressions.fn()
    scores = score_complexity.fn(exprs)
    bins = {s.complexity_bin for s in scores}
    assert len(bins) >= 2  # At least 2 different bins


def test_categorize_by_domain() -> None:
    exprs = simulate_expressions.fn()
    cats = categorize_by_domain.fn(exprs)
    assert "Child Health" in cats
    assert cats["Child Health"] == 2


def test_flow_runs() -> None:
    state = expression_scoring_flow(return_state=True)
    assert state.is_completed()
