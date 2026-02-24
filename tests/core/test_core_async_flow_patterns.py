"""Tests for flow 035 — Async Flow Patterns."""

import asyncio
import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "core_async_flow_patterns",
    Path(__file__).resolve().parent.parent.parent / "flows" / "core" / "core_async_flow_patterns.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["core_async_flow_patterns"] = _mod
_spec.loader.exec_module(_mod)

sync_extract = _mod.sync_extract
async_enrich = _mod.async_enrich
sync_load = _mod.sync_load
enrich_subflow = _mod.enrich_subflow


def test_sync_extract() -> None:
    result = sync_extract.fn()
    assert isinstance(result, list)
    assert len(result) == 4


def test_async_enrich() -> None:
    result = asyncio.run(async_enrich.fn("record-A"))
    assert result == "enriched:record-A"


def test_sync_load() -> None:
    result = sync_load.fn(["enriched:a", "enriched:b"])
    assert isinstance(result, str)
    assert "2" in result


def test_enrich_subflow() -> None:
    result = asyncio.run(enrich_subflow(["r1", "r2"]))
    assert isinstance(result, list)
    assert len(result) == 2
    assert all(r.startswith("enriched:") for r in result)
