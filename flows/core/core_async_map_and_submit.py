"""Async Map and Submit.

Demonstrate .map() and .submit() with async tasks for parallel fan-out.

Airflow equivalent: Dynamic task mapping with deferrable operators.
Prefect approach:    .map() and .submit() work with async tasks for
                     concurrent execution within an async flow.
"""

import asyncio

from dotenv import load_dotenv
from prefect import flow, task


@task
async def async_transform(item: str) -> str:
    """Transform a single item asynchronously.

    Args:
        item: The raw item string.

    Returns:
        The transformed item string.
    """
    await asyncio.sleep(0.05)
    result = f"transformed:{item.upper()}"
    print(f"Transformed: {item} -> {result}")
    return result


@task
async def async_validate(item: str) -> bool:
    """Validate a single item asynchronously.

    Args:
        item: The item string to validate.

    Returns:
        True if the item is valid (non-empty).
    """
    await asyncio.sleep(0.02)
    is_valid = len(item) > 0
    print(f"Validated: {item} -> {is_valid}")
    return is_valid


@task
async def async_summarize(results: list[str]) -> str:
    """Summarize transformed results.

    Args:
        results: List of transformed item strings.

    Returns:
        A summary string.
    """
    msg = f"Summary: {len(results)} items processed — {', '.join(results)}"
    print(msg)
    return msg


@flow(name="core_async_map_and_submit", log_prints=True)
async def async_map_and_submit_flow() -> None:
    """Fan-out with .map() and .submit() using async tasks."""
    items = ["alpha", "beta", "gamma", "delta"]

    # Use .map() for parallel fan-out
    transform_futures = async_transform.map(items)
    transformed = [f.result() for f in transform_futures]

    # Use .submit() for individual async task runs
    validate_futures = [async_validate.submit(item) for item in transformed]  # type: ignore[call-overload]
    validations = [f.result() for f in validate_futures]

    valid_items = [item for item, valid in zip(transformed, validations, strict=True) if valid]
    await async_summarize(valid_items)  # type: ignore[arg-type]


if __name__ == "__main__":
    load_dotenv()
    asyncio.run(async_map_and_submit_flow())
