"""Tests for flow 057 -- Transactions."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "patterns_transactions",
    Path(__file__).resolve().parent.parent.parent / "flows" / "patterns" / "patterns_transactions.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["patterns_transactions"] = _mod
_spec.loader.exec_module(_mod)

step_a = _mod.step_a
step_b = _mod.step_b
step_c = _mod.step_c
summarize_transaction = _mod.summarize_transaction
transactions_flow = _mod.transactions_flow


def test_step_a() -> None:
    result = step_a.fn()
    assert result == "step_a completed"


def test_step_b() -> None:
    result = step_b.fn()
    assert result == "step_b completed"


def test_step_c() -> None:
    result = step_c.fn()
    assert result == "step_c completed"


def test_summarize_transaction() -> None:
    result = summarize_transaction.fn(["a", "b", "c"])
    assert "3 steps" in result


def test_flow_runs() -> None:
    state = transactions_flow(return_state=True)
    assert state.is_completed()
