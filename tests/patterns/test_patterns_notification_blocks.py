"""Tests for Notification Blocks pattern flow."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "patterns_notification_blocks",
    Path(__file__).resolve().parent.parent.parent / "flows" / "patterns" / "patterns_notification_blocks.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["patterns_notification_blocks"] = _mod
_spec.loader.exec_module(_mod)

configure_notification_blocks = _mod.configure_notification_blocks
demonstrate_template_resolution = _mod.demonstrate_template_resolution
process_data = _mod.process_data
notification_blocks_flow = _mod.notification_blocks_flow


def test_configure_notification_blocks() -> None:
    result = configure_notification_blocks.fn()
    assert isinstance(result, dict)
    assert "slack" in result
    assert "custom" in result
    assert result["slack"]["type"] == "SlackWebhook"
    assert result["custom"]["type"] == "CustomWebhookNotificationBlock"


def test_demonstrate_template_resolution() -> None:
    result = demonstrate_template_resolution.fn()
    assert isinstance(result, dict)
    # All placeholders should be resolved
    url = result["url"]
    assert "{{" not in url
    assert "secret-xyz-789" in url
    json_data = result["json"]
    assert "{{" not in str(json_data)
    assert json_data["title"] == "Pipeline Alert"
    assert "150 records" in json_data["message"]


def test_process_data() -> None:
    result = process_data.fn("api")
    assert isinstance(result, dict)
    assert result["source"] == "api"
    assert result["records"] == 150
    assert result["status"] == "success"


def test_flow_runs() -> None:
    state = notification_blocks_flow(return_state=True)
    assert state.is_completed()
