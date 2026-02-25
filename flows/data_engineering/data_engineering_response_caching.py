"""Response Caching.

Application-level response cache with TTL expiry and hit/miss tracking.
Demonstrates caching API responses in a dict with hashlib-based keys.

Airflow equivalent: Forecast accuracy / cached vs fresh comparison (DAG 082).
Prefect approach:    Dict-based cache with TTL, hashlib cache keys.
"""

import hashlib
import time
from typing import Any

from dotenv import load_dotenv
from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class CacheEntry(BaseModel):
    """A single cache entry."""

    key: str
    value: dict[str, Any]
    cached_at: float
    ttl_seconds: int


class CacheStats(BaseModel):
    """Statistics about cache usage."""

    total_requests: int
    hits: int
    misses: int
    hit_rate: float


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def make_cache_key(endpoint: str, params: dict[str, Any]) -> str:
    """Generate a deterministic cache key.

    Args:
        endpoint: API endpoint.
        params: Request parameters.

    Returns:
        Hex digest string as cache key.
    """
    raw = f"{endpoint}:{sorted(params.items())}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


@task
def check_cache(cache: dict[str, Any], key: str, ttl_seconds: int) -> CacheEntry | None:
    """Check if a valid cache entry exists.

    Args:
        cache: The cache dict.
        key: Cache key to look up.
        ttl_seconds: TTL in seconds.

    Returns:
        CacheEntry if valid, None if expired or missing.
    """
    if key not in cache:
        return None
    entry = cache[key]
    age = time.time() - entry["cached_at"]
    if age > ttl_seconds:
        return None
    return CacheEntry(
        key=key,
        value=entry["value"],
        cached_at=entry["cached_at"],
        ttl_seconds=ttl_seconds,
    )


@task
def simulate_api_call(endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
    """Simulate an API call.

    Args:
        endpoint: API endpoint.
        params: Request parameters.

    Returns:
        Simulated response dict.
    """
    return {
        "endpoint": endpoint,
        "params": params,
        "data": {
            "result": f"data_for_{endpoint}",
            "count": sum(hash(str(v)) % 100 for v in params.values()),
        },
        "fetched_at": time.time(),
    }


@task
def fetch_with_cache(
    endpoint: str,
    params: dict[str, Any],
    cache: dict[str, Any],
    ttl_seconds: int = 300,
) -> tuple[dict[str, Any], bool]:
    """Fetch data with caching. Returns cached data on hit.

    Args:
        endpoint: API endpoint.
        params: Request parameters.
        cache: Shared cache dict.
        ttl_seconds: Cache TTL in seconds.

    Returns:
        Tuple of (response_data, cache_hit).
    """
    key = make_cache_key.fn(endpoint, params)
    entry = check_cache.fn(cache, key, ttl_seconds)

    if entry is not None:
        print(f"Cache HIT: {endpoint}")
        return entry.value, True

    print(f"Cache MISS: {endpoint}")
    data = simulate_api_call.fn(endpoint, params)
    cache[key] = {"value": data, "cached_at": time.time()}
    return data, False


@task
def compute_cache_stats(hit_log: list[bool]) -> CacheStats:
    """Compute cache hit/miss statistics.

    Args:
        hit_log: List of booleans (True=hit, False=miss).

    Returns:
        CacheStats.
    """
    hits = sum(1 for h in hit_log if h)
    misses = len(hit_log) - hits
    rate = hits / len(hit_log) if hit_log else 0.0
    return CacheStats(
        total_requests=len(hit_log),
        hits=hits,
        misses=misses,
        hit_rate=round(rate, 2),
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="data_engineering_response_caching", log_prints=True)
def response_caching_flow() -> CacheStats:
    """Demonstrate application-level response caching with TTL.

    Returns:
        CacheStats with hit/miss rates.
    """
    cache: dict[str, Any] = {}
    hit_log: list[bool] = []

    requests: list[tuple[str, dict[str, Any]]] = [
        ("/api/users", {"page": 1}),
        ("/api/products", {"category": "A"}),
        ("/api/users", {"page": 1}),
        ("/api/users", {"page": 2}),
        ("/api/products", {"category": "A"}),
        ("/api/orders", {"status": "pending"}),
        ("/api/users", {"page": 1}),
        ("/api/orders", {"status": "pending"}),
    ]

    for endpoint, params in requests:
        _data, was_hit = fetch_with_cache(endpoint, params, cache, ttl_seconds=300)
        hit_log.append(was_hit)

    stats = compute_cache_stats(hit_log)
    print(f"Cache stats: {stats.hits} hits, {stats.misses} misses ({stats.hit_rate:.0%} hit rate)")
    return stats


if __name__ == "__main__":
    load_dotenv()
    response_caching_flow()
