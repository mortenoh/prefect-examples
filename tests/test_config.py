"""Tests for prefect_examples.config."""

import re

from prefect_examples.config import timestamp


def test_timestamp_returns_iso_format() -> None:
    ts = timestamp()
    assert re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", ts)
