"""Tests for flow 025 — Structured Logging."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "core_structured_logging",
    Path(__file__).resolve().parent.parent.parent / "flows" / "core" / "core_structured_logging.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["core_structured_logging"] = _mod
_spec.loader.exec_module(_mod)

task_with_logger = _mod.task_with_logger
task_with_print = _mod.task_with_print
task_with_extra_context = _mod.task_with_extra_context
structured_logging_flow = _mod.structured_logging_flow


def test_task_with_print() -> None:
    result = task_with_print.fn("beta")
    assert result == "printed:beta"


def test_flow_runs() -> None:
    """get_run_logger() requires Prefect runtime — test via flow."""
    state = structured_logging_flow(return_state=True)
    assert state.is_completed()
