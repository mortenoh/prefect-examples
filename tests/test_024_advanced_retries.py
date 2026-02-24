"""Tests for flow 024 — Advanced Retries."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "flow_024",
    Path(__file__).resolve().parent.parent / "flows" / "024_advanced_retries.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_024"] = _mod
_spec.loader.exec_module(_mod)

backoff_task = _mod.backoff_task
jittery_task = _mod.jittery_task
conditional_retry_task = _mod.conditional_retry_task
retry_on_value_error = _mod.retry_on_value_error


def test_backoff_task_succeeds_when_no_failures() -> None:
    result = backoff_task.fn(fail_count=0)
    assert isinstance(result, str)
    assert "succeeded" in result


def test_jittery_task_succeeds_when_no_failures() -> None:
    result = jittery_task.fn(fail_count=0)
    assert isinstance(result, str)
    assert "succeeded" in result


def test_conditional_retry_task_no_error() -> None:
    result = conditional_retry_task.fn("none")
    assert isinstance(result, str)
    assert "completed" in result


def test_conditional_retry_task_value_error() -> None:
    import pytest

    with pytest.raises(ValueError):
        conditional_retry_task.fn("value")


def test_conditional_retry_task_type_error() -> None:
    import pytest

    with pytest.raises(TypeError):
        conditional_retry_task.fn("type")
