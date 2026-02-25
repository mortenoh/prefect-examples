"""Task Results.

Pass structured data between tasks via return values.

Airflow equivalent: XCom push/pull.
Prefect approach:    return values — no push/pull ceremony.
"""

from typing import Any

from dotenv import load_dotenv
from prefect import flow, task


@task
def produce_metrics() -> dict[str, Any]:
    """Generate a metrics dictionary.

    Returns:
        Dict with keys "total", "average", and "items".
    """
    metrics = {
        "total": 150,
        "average": 37.5,
        "items": ["alpha", "beta", "gamma", "delta"],
    }
    print(f"Produced metrics: {metrics}")
    return metrics


@task
def consume_metrics(metrics: dict[str, Any]) -> str:
    """Summarise the metrics into a human-readable string.

    Args:
        metrics: Dict containing "total", "average", and "items".

    Returns:
        A formatted summary string.
    """
    summary = (
        f"Total: {metrics['total']}, "
        f"Average: {metrics['average']}, "
        f"Items ({len(metrics['items'])}): {', '.join(metrics['items'])}"
    )
    print(summary)
    return summary


@flow(name="basics_task_results", log_prints=True)
def task_results_flow() -> None:
    """Produce metrics, then consume them — accessing individual keys."""
    metrics = produce_metrics()
    consume_metrics(metrics)


if __name__ == "__main__":
    load_dotenv()
    task_results_flow()
