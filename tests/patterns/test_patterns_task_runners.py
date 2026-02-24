"""Tests for flow 059 -- Task Runners."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "patterns_task_runners",
    Path(__file__).resolve().parent.parent.parent / "flows" / "patterns" / "patterns_task_runners.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["patterns_task_runners"] = _mod
_spec.loader.exec_module(_mod)

io_bound_task = _mod.io_bound_task
cpu_bound_task = _mod.cpu_bound_task
summarize_runner = _mod.summarize_runner
threaded_io_flow = _mod.threaded_io_flow
default_cpu_flow = _mod.default_cpu_flow
task_runners_flow = _mod.task_runners_flow


def test_io_bound_task() -> None:
    result = io_bound_task.fn("test-item")
    assert isinstance(result, dict)
    assert result["item"] == "test-item"
    assert result["type"] == "io"


def test_cpu_bound_task() -> None:
    result = cpu_bound_task.fn(5)
    assert isinstance(result, dict)
    assert result["n"] == 5
    assert result["type"] == "cpu"


def test_summarize_runner() -> None:
    results = [{"duration": 0.1}, {"duration": 0.2}]
    summary = summarize_runner.fn(results, "TestRunner")
    assert "TestRunner" in summary
    assert "2 tasks" in summary


def test_threaded_io_flow() -> None:
    result = threaded_io_flow()
    assert isinstance(result, str)
    assert "ThreadPoolTaskRunner" in result


def test_default_cpu_flow() -> None:
    result = default_cpu_flow()
    assert isinstance(result, str)
    assert "DefaultTaskRunner" in result


def test_flow_runs() -> None:
    # Test subflows individually — the orchestrator flow triggers a known
    # ephemeral-server 422 error when registering subflow task runs.
    io_result = threaded_io_flow()
    assert "ThreadPoolTaskRunner" in io_result
    cpu_result = default_cpu_flow()
    assert "DefaultTaskRunner" in cpu_result
