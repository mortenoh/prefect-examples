"""Task Runners.

Compare thread pool and process pool task runners for concurrent execution.

Airflow equivalent: Executors (Local, Celery, Kubernetes) (DAG 048).
Prefect approach:    ThreadPoolTaskRunner for I/O, default for CPU tasks.
"""

import time
from typing import Any

from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def io_bound_task(item: str) -> dict[str, Any]:
    """Simulate an I/O-bound task (network call, file read).

    Args:
        item: The item to process.

    Returns:
        A dict with the result and timing.
    """
    start = time.monotonic()
    time.sleep(0.05)
    duration = round(time.monotonic() - start, 3)
    result = {"item": item, "duration": duration, "type": "io"}
    print(f"I/O task {item}: {duration}s")
    return result


@task
def cpu_bound_task(n: int) -> dict[str, Any]:
    """Simulate a CPU-bound task (computation).

    Args:
        n: Input number for computation.

    Returns:
        A dict with the computation result.
    """
    start = time.monotonic()
    total = sum(i * i for i in range(n * 1000))
    duration = round(time.monotonic() - start, 4)
    result = {"n": n, "total": total, "duration": duration, "type": "cpu"}
    print(f"CPU task n={n}: {duration}s")
    return result


@task
def summarize_runner(results: list[dict[str, Any]], runner_name: str) -> str:
    """Summarize task runner results.

    Args:
        results: The list of task results.
        runner_name: The name of the runner used.

    Returns:
        A summary string.
    """
    total_duration = sum(r.get("duration", 0) for r in results)
    summary = f"{runner_name}: {len(results)} tasks, total_duration={total_duration:.3f}s"
    print(summary)
    return summary


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------


@flow(name="patterns_threaded_io", log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=3))  # type: ignore[arg-type]
def threaded_io_flow() -> str:
    """Run I/O-bound tasks with ThreadPoolTaskRunner for concurrency.

    Returns:
        A summary string.
    """
    items = ["api-call-1", "api-call-2", "api-call-3", "api-call-4"]
    futures = io_bound_task.map(items)
    results = [f.result() for f in futures]
    return summarize_runner(results, "ThreadPoolTaskRunner")


@flow(name="patterns_default_cpu", log_prints=True)
def default_cpu_flow() -> str:
    """Run CPU-bound tasks with the default task runner.

    Returns:
        A summary string.
    """
    inputs = [10, 20, 30, 40]
    futures = cpu_bound_task.map(inputs)
    results = [f.result() for f in futures]
    return summarize_runner(results, "DefaultTaskRunner")


@flow(name="patterns_task_runners", log_prints=True)
def task_runners_flow() -> None:
    """Compare task runner behaviour for I/O vs CPU workloads."""
    io_summary = threaded_io_flow()
    cpu_summary = default_cpu_flow()
    print(f"I/O: {io_summary}")
    print(f"CPU: {cpu_summary}")


if __name__ == "__main__":
    task_runners_flow()
