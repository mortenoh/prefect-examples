"""034 — Concurrent Async.

Demonstrate concurrent task execution with asyncio.gather().

Airflow equivalent: Multiple deferrable operators running in parallel.
Prefect approach:    asyncio.gather() to run async tasks concurrently,
                     achieving wall-clock time ≈ max(delays) not sum.
"""

import asyncio
import time

from prefect import flow, task


@task
async def fetch_endpoint(name: str, delay: float = 0.5) -> dict:
    """Simulate fetching data from an API endpoint.

    Args:
        name: The endpoint name.
        delay: Simulated response time in seconds.

    Returns:
        A dict with the endpoint name and simulated record count.
    """
    print(f"Fetching {name} (delay={delay}s) ...")
    await asyncio.sleep(delay)
    result = {"endpoint": name, "records": len(name) * 10, "delay": delay}
    print(f"Fetched {name}: {result['records']} records")
    return result


@task
async def aggregate_results(results: list[dict]) -> str:
    """Aggregate results from multiple endpoint fetches.

    Args:
        results: List of endpoint response dicts.

    Returns:
        A summary string.
    """
    total_records = sum(r["records"] for r in results)
    endpoints = [r["endpoint"] for r in results]
    msg = f"Aggregated {total_records} records from {', '.join(endpoints)}"
    print(msg)
    return msg


@flow(name="034_concurrent_async", log_prints=True)
async def concurrent_async_flow() -> None:
    """Fetch multiple endpoints concurrently and aggregate results.

    Total wall-clock time ≈ max(delays), not sum(delays), because
    asyncio.gather() runs all fetches concurrently.
    """
    start = time.monotonic()

    results = await asyncio.gather(
        fetch_endpoint("users", delay=0.3),
        fetch_endpoint("orders", delay=0.5),
        fetch_endpoint("products", delay=0.2),
    )

    elapsed = time.monotonic() - start
    print(f"All fetches completed in {elapsed:.2f}s (concurrent)")

    await aggregate_results(list(results))


if __name__ == "__main__":
    asyncio.run(concurrent_async_flow())
