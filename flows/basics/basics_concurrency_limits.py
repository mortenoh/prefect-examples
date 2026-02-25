"""Concurrency Limits.

Throttle parallel task execution with named concurrency limits.

Airflow equivalent: pool slots.
Prefect approach:    concurrency() context manager with named limits.
"""

import time

from dotenv import load_dotenv
from prefect import flow, task
from prefect.concurrency.sync import concurrency


@task
def limited_task(item: str) -> str:
    """Process a single item under a concurrency limit.

    Args:
        item: The item to process.

    Returns:
        A string confirming the item was processed.
    """
    with concurrency("demo-limit", occupy=1):
        print(f"Processing {item!r} ...")
        time.sleep(0.5)
    result = f"processed:{item}"
    print(f"Done: {result}")
    return result


@flow(name="basics_concurrency_limits", log_prints=True)
def concurrency_limits_flow() -> None:
    """Map work items through a concurrency-limited task."""
    items = ["alpha", "beta", "gamma", "delta", "epsilon"]
    print(f"Submitting {len(items)} items with concurrency limit")
    results = limited_task.map(items)
    print(f"All done: {[r.result() for r in results]}")


if __name__ == "__main__":
    load_dotenv()
    concurrency_limits_flow()
