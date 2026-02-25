"""Task Factories.

Create reusable tasks dynamically with factory functions and decorators.

Airflow equivalent: Custom operators, @task.bash variants (DAGs 013, 047).
Prefect approach:    Python factory functions that return @task-decorated callables.
"""

from collections.abc import Callable
from typing import Any

from prefect import flow, task

# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------


def make_extractor(source: str) -> Callable[[], dict[str, Any]]:
    """Create an extraction task for the given data source.

    Args:
        source: The name of the data source to extract from.

    Returns:
        A Prefect task that extracts data from the named source.
    """

    @task(name=f"extract_{source}")
    def extract() -> dict[str, Any]:
        data = {"source": source, "records": [f"{source}_record_{i}" for i in range(3)]}
        print(f"Extracted {len(data['records'])} records from {source}")
        return data

    return extract


def retry_task(fn: Callable[..., Any], retries: int = 3) -> Callable[..., Any]:
    """Wrap a callable as a Prefect task with retry configuration.

    Args:
        fn: The function to wrap.
        retries: Number of retry attempts.

    Returns:
        A Prefect task with retries configured.
    """
    wrapped: Callable[..., Any] = task(fn, retries=retries, retry_delay_seconds=1)  # type: ignore[call-overload]
    return wrapped


# ---------------------------------------------------------------------------
# Factory-generated tasks
# ---------------------------------------------------------------------------

extract_api = make_extractor("api")
extract_database = make_extractor("database")
extract_file = make_extractor("file")


# ---------------------------------------------------------------------------
# Additional tasks
# ---------------------------------------------------------------------------


@task
def combine_extracts(extracts: list[dict[str, Any]]) -> str:
    """Combine results from multiple extraction tasks.

    Args:
        extracts: A list of extraction result dicts.

    Returns:
        A summary string.
    """
    total = sum(len(e.get("records", [])) for e in extracts)
    sources = [e["source"] for e in extracts]
    summary = f"Combined {total} records from {', '.join(sources)}"
    print(summary)
    return summary


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="patterns_task_factories", log_prints=True)
def task_factories_flow() -> None:
    """Demonstrate factory-generated tasks for reusable extraction patterns."""
    api_data = extract_api()
    db_data = extract_database()
    file_data = extract_file()
    combine_extracts([api_data, db_data, file_data])


if __name__ == "__main__":
    task_factories_flow()
