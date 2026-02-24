"""021 — Task Caching.

Demonstrate task-level caching to avoid redundant computation.

Airflow equivalent: Custom caching logic or external cache (Redis, etc.).
Prefect approach:    cache_policy on @task (INPUTS, TASK_SOURCE, cache_key_fn).
"""

import datetime

from prefect import flow, task
from prefect.cache_policies import INPUTS, TASK_SOURCE


@task(cache_policy=INPUTS, cache_expiration=datetime.timedelta(minutes=5))
def expensive_computation(x: int, y: int) -> int:
    """Simulate an expensive computation that benefits from caching.

    Results are cached based on the input arguments. Identical inputs
    within the expiration window return the cached result.

    Args:
        x: First operand.
        y: Second operand.

    Returns:
        The product of x and y.
    """
    print(f"Computing {x} * {y} (expensive!)")
    result = x * y
    return result


@task(cache_policy=TASK_SOURCE + INPUTS)
def compound_cache_task(data: str) -> str:
    """Cache based on both the task source code and its inputs.

    If either the task body or the input data changes, the cache is
    invalidated.

    Args:
        data: Input string to process.

    Returns:
        An uppercase version of the input.
    """
    result = data.upper()
    print(f"Processed: {data!r} -> {result!r}")
    return result


def _category_cache_key(context: dict, parameters: dict) -> str:  # type: ignore[type-arg]
    """Build a cache key from category and item_id parameters."""
    return f"{parameters['category']}:{parameters['item_id']}"


@task(cache_key_fn=_category_cache_key, cache_expiration=datetime.timedelta(minutes=10))
def cached_lookup(category: str, item_id: int) -> dict:
    """Look up an item with a custom cache key.

    The cache key is built from category and item_id so that
    identical lookups are served from cache.

    Args:
        category: The item category.
        item_id: The item identifier.

    Returns:
        A dict with item details.
    """
    print(f"Looking up {category}/{item_id}")
    return {"category": category, "item_id": item_id, "name": f"{category}-{item_id}", "cached": False}


@flow(name="021_task_caching", log_prints=True)
def task_caching_flow() -> None:
    """Demonstrate various caching strategies by calling tasks twice.

    Cache hits are only visible in Prefect runtime — running tasks
    with .fn() always executes the underlying function.
    """
    # INPUTS-based caching — same args → cache hit
    result1 = expensive_computation(3, 7)
    result2 = expensive_computation(3, 7)
    print(f"expensive_computation results: {result1}, {result2}")

    # TASK_SOURCE + INPUTS caching
    r1 = compound_cache_task("hello")
    r2 = compound_cache_task("hello")
    print(f"compound_cache_task results: {r1}, {r2}")

    # Custom cache key function
    item1 = cached_lookup("widgets", 42)
    item2 = cached_lookup("widgets", 42)
    print(f"cached_lookup results: {item1}, {item2}")


if __name__ == "__main__":
    task_caching_flow()
