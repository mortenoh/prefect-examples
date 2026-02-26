"""Task Results.

Pass structured data between tasks via return values.

Airflow equivalent: XCom push/pull.
Prefect approach:    return values — no push/pull ceremony.
"""

from dotenv import load_dotenv
from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class Metrics(BaseModel):
    """Structured metrics payload passed between tasks."""

    total: int
    average: float
    items: list[str]


@task
def produce_metrics() -> Metrics:
    """Generate a metrics object.

    Returns:
        Metrics with total, average, and items.
    """
    metrics = Metrics(
        total=150,
        average=37.5,
        items=["alpha", "beta", "gamma", "delta"],
    )
    print(f"Produced metrics: {metrics}")
    return metrics


@task
def consume_metrics(metrics: Metrics) -> str:
    """Summarise the metrics into a human-readable string.

    Args:
        metrics: Structured metrics payload.

    Returns:
        A formatted summary string.
    """
    summary = (
        f"Total: {metrics.total}, "
        f"Average: {metrics.average}, "
        f"Items ({len(metrics.items)}): {', '.join(metrics.items)}"
    )
    print(summary)
    return summary


@flow(name="basics_task_results", log_prints=True)
def task_results_flow() -> None:
    """Produce metrics, then consume them — accessing individual fields."""
    metrics = produce_metrics()
    consume_metrics(metrics)


if __name__ == "__main__":
    load_dotenv()
    task_results_flow()
