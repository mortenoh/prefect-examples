"""Tests for flow 032 — Custom Blocks."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "core_custom_blocks",
    Path(__file__).resolve().parent.parent.parent / "flows" / "core" / "core_custom_blocks.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["core_custom_blocks"] = _mod
_spec.loader.exec_module(_mod)

DatabaseConfig = _mod.DatabaseConfig
NotificationConfig = _mod.NotificationConfig
connect_database = _mod.connect_database
send_notification = _mod.send_notification
custom_blocks_flow = _mod.custom_blocks_flow


def test_database_config_defaults() -> None:
    config = DatabaseConfig()
    assert config.host == "localhost"
    assert config.port == 5432
    assert config.database == "mydb"


def test_connect_database() -> None:
    config = DatabaseConfig(host="db.test.com", database="testdb", username="tester")
    result = connect_database.fn(config)
    assert isinstance(result, str)
    assert "db.test.com" in result
    assert "testdb" in result


def test_notification_config_defaults() -> None:
    config = NotificationConfig()
    assert config.channel == "slack"
    assert config.enabled is True


def test_send_notification_enabled() -> None:
    config = NotificationConfig(channel="email", recipient="admin@test.com")
    result = send_notification.fn(config, "Test message")
    assert "email" in result
    assert "Test message" in result


def test_send_notification_disabled() -> None:
    config = NotificationConfig(enabled=False)
    result = send_notification.fn(config, "Skipped message")
    assert "disabled" in result.lower()


def test_flow_runs() -> None:
    state = custom_blocks_flow(return_state=True)
    assert state.is_completed()
