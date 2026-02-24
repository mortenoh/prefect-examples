"""028 — Result Persistence.

Demonstrate result persistence for tasks and flows.

Airflow equivalent: XCom backend configuration, custom result backends.
Prefect approach:    persist_result=True on @task/@flow,
                     result_storage_key for stable keys.
"""

import statistics

from prefect import flow, task


@task(persist_result=True)
def compute_metrics(data: list[int]) -> dict:
    """Compute summary metrics from a list of integers.

    Results are persisted so they survive restarts and can be
    referenced by downstream tasks or re-runs.

    Args:
        data: A list of integers to summarise.

    Returns:
        A dict with total, mean, and median.
    """
    metrics = {
        "total": sum(data),
        "mean": round(statistics.mean(data), 2),
        "median": round(statistics.median(data), 2),
        "count": len(data),
    }
    print(f"Computed metrics: {metrics}")
    return metrics


@task(persist_result=True, result_storage_key="latest-summary-{parameters[label]}")
def build_summary(metrics: dict, label: str) -> str:
    """Build a human-readable summary from metrics.

    The result_storage_key includes the label parameter, making it
    easy to look up the latest summary for a specific label.

    Args:
        metrics: The computed metrics dict.
        label: A label for this summary (used in the storage key).

    Returns:
        A formatted summary string.
    """
    summary = (
        f"[{label}] Total: {metrics['total']}, "
        f"Mean: {metrics['mean']}, "
        f"Median: {metrics['median']} "
        f"(n={metrics['count']})"
    )
    print(summary)
    return summary


@task
def transient_task() -> str:
    """A task without result persistence.

    Results are only available during the current flow run and are
    not stored for later retrieval.

    Returns:
        A transient message.
    """
    msg = "transient result — not persisted"
    print(msg)
    return msg


@flow(name="028_result_persistence", log_prints=True, persist_result=True)
def result_persistence_flow() -> None:
    """Demonstrate persisted vs transient task results."""
    data = [10, 20, 30, 40, 50, 25, 35, 45]
    metrics = compute_metrics(data)
    build_summary(metrics, "daily-report")
    transient_task()


if __name__ == "__main__":
    result_persistence_flow()
