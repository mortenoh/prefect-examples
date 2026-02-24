"""Tests for flow 027 — Flow Run Names."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "core_flow_run_names",
    Path(__file__).resolve().parent.parent.parent / "flows" / "core" / "core_flow_run_names.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["core_flow_run_names"] = _mod
_spec.loader.exec_module(_mod)

generate_report = _mod.generate_report
template_named_flow = _mod.template_named_flow
callable_named_flow = _mod.callable_named_flow


def test_generate_report() -> None:
    result = generate_report.fn("staging", "2025-01-15")
    assert isinstance(result, str)
    assert "staging" in result
    assert "2025-01-15" in result


def test_template_named_flow() -> None:
    result = template_named_flow("prod", "2025-06-01")
    assert isinstance(result, str)
    assert "prod" in result


def test_callable_named_flow() -> None:
    result = callable_named_flow()
    assert isinstance(result, str)
    assert "completed" in result
