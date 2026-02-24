"""Async Tasks.

Demonstrate async task and flow definitions with sequential awaiting.

Airflow equivalent: Deferrable operators (async sensor pattern).
Prefect approach:    async def tasks and flows, awaited sequentially.
"""

import asyncio

from prefect import flow, task


@task
async def async_fetch(url: str) -> dict:
    """Simulate an async HTTP fetch.

    Args:
        url: The URL to fetch (simulated).

    Returns:
        A dict with the simulated response data.
    """
    print(f"Fetching {url} ...")
    await asyncio.sleep(0.1)  # Simulate network delay
    result = {"url": url, "status": 200, "data": f"Response from {url}"}
    print(f"Fetched {url} — status {result['status']}")
    return result


@task
async def async_process(data: dict) -> str:
    """Process fetched data asynchronously.

    Args:
        data: The response data to process.

    Returns:
        A processed result string.
    """
    print(f"Processing data from {data['url']} ...")
    await asyncio.sleep(0.05)  # Simulate processing time
    result = f"processed:{data['data']}"
    print(f"Processing complete: {result}")
    return result


@flow(name="core_async_tasks", log_prints=True)
async def async_tasks_flow() -> None:
    """Run async tasks sequentially with await."""
    response = await async_fetch("https://api.example.com/users")
    await async_process(response)

    response2 = await async_fetch("https://api.example.com/orders")
    await async_process(response2)


if __name__ == "__main__":
    asyncio.run(async_tasks_flow())
