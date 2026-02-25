"""HTTP Tasks.

Make HTTP requests from Prefect tasks using httpx.

Airflow equivalent: HttpOperator, HttpSensor (DAGs 033-034).
Prefect approach:    httpx in a @task -- no special operator needed.
"""

from typing import Any

import httpx
from prefect import flow, task

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def http_get(url: str) -> dict[str, Any]:
    """Perform an HTTP GET request and return the JSON response.

    Args:
        url: The URL to fetch.

    Returns:
        The JSON response as a dict.
    """
    response = httpx.get(url, timeout=10.0)
    response.raise_for_status()
    data: dict[str, Any] = response.json()
    print(f"GET {url} -> {response.status_code}")
    return data


@task
def http_post(url: str, data: dict[str, Any]) -> dict[str, Any]:
    """Perform an HTTP POST request and return the JSON response.

    Args:
        url: The URL to post to.
        data: The JSON payload to send.

    Returns:
        The JSON response as a dict.
    """
    response = httpx.post(url, json=data, timeout=10.0)
    response.raise_for_status()
    result: dict[str, Any] = response.json()
    print(f"POST {url} -> {response.status_code}")
    return result


@task
def check_endpoint(url: str) -> bool:
    """Check if an HTTP endpoint is reachable.

    Args:
        url: The URL to check.

    Returns:
        True if the endpoint returns a 2xx status, False otherwise.
    """
    try:
        response = httpx.get(url, timeout=5.0)
        healthy = response.is_success
    except httpx.HTTPError:
        healthy = False
    print(f"Health check {url}: {'OK' if healthy else 'FAIL'}")
    return healthy


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="patterns_http_tasks", log_prints=True)
def http_tasks_flow() -> None:
    """Demonstrate HTTP GET, POST, and health checks."""
    get_result = http_get("https://httpbin.org/get")
    print(f"GET origin: {get_result.get('origin', 'unknown')}")

    post_result = http_post("https://httpbin.org/post", {"key": "value"})
    print(f"POST echoed json: {post_result.get('json', {})}")

    check_endpoint("https://httpbin.org/status/200")


if __name__ == "__main__":
    http_tasks_flow()
