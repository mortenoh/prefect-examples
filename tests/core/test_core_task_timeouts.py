"""Tests for flow 022 — Task Timeouts."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "core_task_timeouts",
    Path(__file__).resolve().parent.parent.parent / "flows" / "core" / "core_task_timeouts.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["core_task_timeouts"] = _mod
_spec.loader.exec_module(_mod)

quick_task = _mod.quick_task
cleanup_task = _mod.cleanup_task


def test_quick_task() -> None:
    result = quick_task.fn()
    assert isinstance(result, str)
    assert "completed" in result


def test_cleanup_task_normal() -> None:
    result = cleanup_task.fn(timed_out=False)
    assert "normal cleanup" in result


def test_cleanup_task_timeout() -> None:
    result = cleanup_task.fn(timed_out=True)
    assert "recovering from timeout" in result
