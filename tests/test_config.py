"""Tests for prefect_examples.config."""

import re

from prefect_examples.config import FLOW_DEFAULTS, TASK_DEFAULTS, timestamp


def test_flow_defaults_has_required_keys() -> None:
    for key in ("retries", "retry_delay_seconds", "log_prints"):
        assert key in FLOW_DEFAULTS


def test_task_defaults_has_required_keys() -> None:
    for key in ("retries", "retry_delay_seconds", "log_prints"):
        assert key in TASK_DEFAULTS


def test_timestamp_returns_iso_format() -> None:
    ts = timestamp()
    assert re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", ts)
