"""Tests for flow 033 — Async Tasks."""

import asyncio
import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "flow_033",
    Path(__file__).resolve().parent.parent / "flows" / "033_async_tasks.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_033"] = _mod
_spec.loader.exec_module(_mod)

async_fetch = _mod.async_fetch
async_process = _mod.async_process
async_tasks_flow = _mod.async_tasks_flow


def test_async_fetch() -> None:
    result = asyncio.run(async_fetch.fn("https://api.example.com/test"))
    assert isinstance(result, dict)
    assert result["url"] == "https://api.example.com/test"
    assert result["status"] == 200


def test_async_process() -> None:
    data = {"url": "https://example.com", "status": 200, "data": "test-data"}
    result = asyncio.run(async_process.fn(data))
    assert isinstance(result, str)
    assert "processed:" in result


def test_flow_runs() -> None:
    state = asyncio.run(async_tasks_flow(return_state=True))
    assert state.is_completed()
