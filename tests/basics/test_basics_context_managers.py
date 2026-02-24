"""Tests for flow 019 — Context Managers."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "basics_context_managers",
    Path(__file__).resolve().parent.parent.parent / "flows" / "basics" / "basics_context_managers.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["basics_context_managers"] = _mod
_spec.loader.exec_module(_mod)

setup_resource = _mod.setup_resource
use_resource = _mod.use_resource
cleanup_resource = _mod.cleanup_resource
context_managers_flow = _mod.context_managers_flow


def test_setup_resource_returns_string() -> None:
    result = setup_resource.fn()
    assert isinstance(result, str)


def test_use_resource_returns_string() -> None:
    result = use_resource.fn("resource-1")
    assert isinstance(result, str)


def test_flow_runs() -> None:
    state = context_managers_flow(return_state=True)
    assert state.is_completed()
