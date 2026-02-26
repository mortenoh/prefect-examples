"""Shared configuration defaults for all example flows."""

import datetime


def timestamp() -> str:
    """Return the current UTC timestamp as an ISO-8601 string."""
    return datetime.datetime.now(tz=datetime.UTC).isoformat()
