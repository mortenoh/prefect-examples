"""Tests for flow 072 -- Response Caching."""

import importlib.util
import sys
import time
from pathlib import Path
from typing import Any

_spec = importlib.util.spec_from_file_location(
    "data_engineering_response_caching",
    Path(__file__).resolve().parent.parent.parent
    / "flows"
    / "data_engineering"
    / "data_engineering_response_caching.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["data_engineering_response_caching"] = _mod
_spec.loader.exec_module(_mod)

CacheEntry = _mod.CacheEntry
CacheStats = _mod.CacheStats
make_cache_key = _mod.make_cache_key
check_cache = _mod.check_cache
simulate_api_call = _mod.simulate_api_call
fetch_with_cache = _mod.fetch_with_cache
compute_cache_stats = _mod.compute_cache_stats
response_caching_flow = _mod.response_caching_flow


def test_cache_key_deterministic() -> None:
    k1 = make_cache_key.fn("/api/test", {"a": 1})
    k2 = make_cache_key.fn("/api/test", {"a": 1})
    assert k1 == k2


def test_cache_key_different() -> None:
    k1 = make_cache_key.fn("/api/a", {"x": 1})
    k2 = make_cache_key.fn("/api/b", {"x": 1})
    assert k1 != k2


def test_check_cache_miss() -> None:
    result = check_cache.fn({}, "nonexistent", 300)
    assert result is None


def test_check_cache_hit() -> None:
    cache = {"key1": {"value": {"data": 1}, "cached_at": time.time()}}
    result = check_cache.fn(cache, "key1", 300)
    assert result is not None
    assert result.value == {"data": 1}


def test_fetch_with_cache_miss_then_hit() -> None:
    cache: dict[str, Any] = {}
    _, hit1 = fetch_with_cache.fn("/api/test", {"a": 1}, cache, 300)
    assert hit1 is False
    _, hit2 = fetch_with_cache.fn("/api/test", {"a": 1}, cache, 300)
    assert hit2 is True


def test_compute_cache_stats() -> None:
    stats = compute_cache_stats.fn([True, False, True, True, False])
    assert stats.hits == 3
    assert stats.misses == 2
    assert stats.hit_rate == 0.6


def test_flow_runs() -> None:
    state = response_caching_flow(return_state=True)
    assert state.is_completed()
