"""Tests for flow 034 — Concurrent Async."""

import asyncio
import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "flow_034",
    Path(__file__).resolve().parent.parent / "flows" / "034_concurrent_async.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_034"] = _mod
_spec.loader.exec_module(_mod)

fetch_endpoint = _mod.fetch_endpoint
aggregate_results = _mod.aggregate_results
concurrent_async_flow = _mod.concurrent_async_flow


def test_fetch_endpoint() -> None:
    result = asyncio.run(fetch_endpoint.fn("users", delay=0.01))
    assert isinstance(result, dict)
    assert result["endpoint"] == "users"
    assert "records" in result


def test_aggregate_results() -> None:
    results = [
        {"endpoint": "users", "records": 50, "delay": 0.1},
        {"endpoint": "orders", "records": 60, "delay": 0.1},
    ]
    result = asyncio.run(aggregate_results.fn(results))
    assert isinstance(result, str)
    assert "110" in result  # 50 + 60


def test_flow_runs() -> None:
    state = asyncio.run(concurrent_async_flow(return_state=True))
    assert state.is_completed()
