"""Shared configuration defaults for all example flows."""

import datetime

FLOW_DEFAULTS: dict[str, object] = {
    "retries": 1,
    "retry_delay_seconds": 10,
    "log_prints": True,
}

TASK_DEFAULTS: dict[str, object] = {
    "retries": 1,
    "retry_delay_seconds": 10,
    "log_prints": True,
}


def timestamp() -> str:
    """Return the current UTC timestamp as an ISO-8601 string."""
    return datetime.datetime.now(tz=datetime.UTC).isoformat()
