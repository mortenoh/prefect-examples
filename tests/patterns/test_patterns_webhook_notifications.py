"""Tests for flow 049 -- Webhook Notifications."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "patterns_webhook_notifications",
    Path(__file__).resolve().parent.parent.parent / "flows" / "patterns" / "patterns_webhook_notifications.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["patterns_webhook_notifications"] = _mod
_spec.loader.exec_module(_mod)

send_notification = _mod.send_notification
process_data = _mod.process_data
webhook_notifications_flow = _mod.webhook_notifications_flow


def test_send_notification() -> None:
    result = send_notification.fn("test.event", {"key": "value"})
    assert isinstance(result, dict)
    assert result["event"] == "test.event"
    assert result["status"] == "sent"


def test_process_data() -> None:
    result = process_data.fn()
    assert isinstance(result, dict)
    assert result["records_processed"] == 42
    assert result["status"] == "success"


def test_flow_runs() -> None:
    state = webhook_notifications_flow(return_state=True)
    assert state.is_completed()
