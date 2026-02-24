"""Tests for flow 001 — Hello World."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "flow_001",
    Path(__file__).resolve().parent.parent / "flows" / "001_hello_world.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_001"] = _mod
_spec.loader.exec_module(_mod)

say_hello = _mod.say_hello
print_date = _mod.print_date
hello_world = _mod.hello_world


def test_say_hello_returns_greeting() -> None:
    result = say_hello.fn()
    assert result == "Hello from Prefect!"


def test_print_date_returns_string() -> None:
    result = print_date.fn()
    assert isinstance(result, str)
    assert len(result) > 0


def test_hello_world_flow_runs() -> None:
    state = hello_world(return_state=True)
    assert state.is_completed()
