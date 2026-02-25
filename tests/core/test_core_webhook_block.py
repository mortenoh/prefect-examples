"""Tests for Webhook Block flow."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally -- use importlib.
_spec = importlib.util.spec_from_file_location(
    "core_webhook_block",
    Path(__file__).resolve().parent.parent.parent / "flows" / "core" / "core_webhook_block.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["core_webhook_block"] = _mod
_spec.loader.exec_module(_mod)

create_get_webhook = _mod.create_get_webhook
create_post_webhook = _mod.create_post_webhook
simulate_webhook_call = _mod.simulate_webhook_call
demonstrate_save_load_pattern = _mod.demonstrate_save_load_pattern
webhook_block_flow = _mod.webhook_block_flow


def test_create_get_webhook() -> None:
    result = create_get_webhook.fn()
    assert result["method"] == "GET"
    assert result["url_host"] == "api.example.com"
    assert "Accept" in result["header_keys"]


def test_create_post_webhook() -> None:
    result = create_post_webhook.fn()
    assert result["method"] == "POST"
    assert "Authorization" in result["header_keys"]
    assert "Content-Type" in result["header_keys"]


def test_simulate_webhook_call_get() -> None:
    result = simulate_webhook_call.fn(method="GET", url_host="example.com")
    assert result["method"] == "GET"
    assert result["simulated"] is True
    assert result["payload"] is None


def test_simulate_webhook_call_post() -> None:
    payload = {"event": "test"}
    result = simulate_webhook_call.fn(method="POST", url_host="example.com", payload=payload)
    assert result["method"] == "POST"
    assert result["payload"] == payload


def test_demonstrate_save_load_pattern() -> None:
    result = demonstrate_save_load_pattern.fn()
    assert "save" in result
    assert "load" in result
    assert "call" in result


def test_flow_runs() -> None:
    state = webhook_block_flow(return_state=True)
    assert state.is_completed()
