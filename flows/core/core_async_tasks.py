"""Async Tasks.

Demonstrate async task and flow definitions with sequential awaiting.

Airflow equivalent: Deferrable operators (async sensor pattern).
Prefect approach:    async def tasks and flows, awaited sequentially.
"""

import asyncio

from dotenv import load_dotenv
from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class HttpResponse(BaseModel):
    """Simulated HTTP response returned by async_fetch."""

    url: str
    status: int
    data: str


@task
async def async_fetch(url: str) -> HttpResponse:
    """Simulate an async HTTP fetch.

    Args:
        url: The URL to fetch (simulated).

    Returns:
        An HttpResponse with the simulated response data.
    """
    print(f"Fetching {url} ...")
    await asyncio.sleep(0.1)  # Simulate network delay
    result = HttpResponse(url=url, status=200, data=f"Response from {url}")
    print(f"Fetched {url} -- status {result.status}")
    return result


@task
async def async_process(data: HttpResponse) -> str:
    """Process fetched data asynchronously.

    Args:
        data: The HTTP response to process.

    Returns:
        A processed result string.
    """
    print(f"Processing data from {data.url} ...")
    await asyncio.sleep(0.05)  # Simulate processing time
    result = f"processed:{data.data}"
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
    load_dotenv()
    asyncio.run(async_tasks_flow())
