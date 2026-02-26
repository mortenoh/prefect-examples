"""Tests for flow 017 -- Variables and Params."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally -- use importlib.
_spec = importlib.util.spec_from_file_location(
    "basics_variables_and_params",
    Path(__file__).resolve().parent.parent.parent / "flows" / "basics" / "basics_variables_and_params.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["basics_variables_and_params"] = _mod
_spec.loader.exec_module(_mod)

AppConfig = _mod.AppConfig
process_with_config = _mod.process_with_config
variables_flow = _mod.variables_flow


def test_process_with_config_returns_string() -> None:
    result = process_with_config.fn(AppConfig(debug=True, batch_size=50), "dev")
    assert isinstance(result, str)
    assert "debug=True" in result
    assert "batch_size=50" in result


def test_flow_runs() -> None:
    state = variables_flow(return_state=True)
    assert state.is_completed()
