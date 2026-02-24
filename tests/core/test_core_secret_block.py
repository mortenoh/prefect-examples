"""Tests for flow 031 — Secret Block."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "core_secret_block",
    Path(__file__).resolve().parent.parent.parent / "flows" / "core" / "core_secret_block.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["core_secret_block"] = _mod
_spec.loader.exec_module(_mod)

get_api_key = _mod.get_api_key
call_api = _mod.call_api
secret_block_flow = _mod.secret_block_flow


def test_get_api_key_fallback() -> None:
    result = get_api_key.fn()
    assert isinstance(result, str)
    assert result == "dev-fallback-key-12345"


def test_call_api() -> None:
    result = call_api.fn("test-key-1234", "/api/v1/test")
    assert isinstance(result, dict)
    assert result["endpoint"] == "/api/v1/test"
    assert result["status"] == 200


def test_flow_runs() -> None:
    state = secret_block_flow(return_state=True)
    assert state.is_completed()
