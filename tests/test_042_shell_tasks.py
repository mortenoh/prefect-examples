"""Tests for flow 042 -- Shell Tasks."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_042",
    Path(__file__).resolve().parent.parent / "flows" / "042_shell_tasks.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_042"] = _mod
_spec.loader.exec_module(_mod)

run_command = _mod.run_command
run_script = _mod.run_script
capture_output = _mod.capture_output
shell_tasks_flow = _mod.shell_tasks_flow


def test_run_command() -> None:
    result = run_command.fn("echo 'hello'")
    assert result == "hello"


def test_run_script() -> None:
    result = run_script.fn("echo 'a'\necho 'b'")
    assert "a" in result
    assert "b" in result


def test_capture_output_success() -> None:
    result = capture_output.fn("echo 'test'")
    assert isinstance(result, dict)
    assert result["stdout"] == "test"
    assert result["returncode"] == 0


def test_capture_output_failure() -> None:
    result = capture_output.fn("exit 1")
    assert result["returncode"] == 1


def test_flow_runs() -> None:
    state = shell_tasks_flow(return_state=True)
    assert state.is_completed()
