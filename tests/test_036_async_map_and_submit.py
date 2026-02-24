"""Tests for flow 036 — Async Map and Submit."""

import asyncio
import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "flow_036",
    Path(__file__).resolve().parent.parent / "flows" / "036_async_map_and_submit.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_036"] = _mod
_spec.loader.exec_module(_mod)

async_transform = _mod.async_transform
async_validate = _mod.async_validate
async_summarize = _mod.async_summarize


def test_async_transform() -> None:
    result = asyncio.run(async_transform.fn("alpha"))
    assert result == "transformed:ALPHA"


def test_async_validate() -> None:
    result = asyncio.run(async_validate.fn("hello"))
    assert result is True


def test_async_validate_empty() -> None:
    result = asyncio.run(async_validate.fn(""))
    assert result is False


def test_async_summarize() -> None:
    result = asyncio.run(async_summarize.fn(["item1", "item2"]))
    assert isinstance(result, str)
    assert "2 items" in result
