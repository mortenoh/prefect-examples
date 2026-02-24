"""048 -- SLA Monitoring.

Track task durations and compare against SLA thresholds.

Airflow equivalent: SLA miss detection, execution_timeout (DAG 073).
Prefect approach:    time.monotonic() for timing, threshold comparison.
"""

import time

from prefect import flow, task

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def fast_task() -> dict:
    """A task that completes quickly.

    Returns:
        A dict with task name and duration.
    """
    start = time.monotonic()
    time.sleep(0.01)
    duration = time.monotonic() - start
    print(f"fast_task completed in {duration:.3f}s")
    return {"task": "fast_task", "duration": round(duration, 3)}


@task
def medium_task() -> dict:
    """A task that takes a moderate amount of time.

    Returns:
        A dict with task name and duration.
    """
    start = time.monotonic()
    time.sleep(0.05)
    duration = time.monotonic() - start
    print(f"medium_task completed in {duration:.3f}s")
    return {"task": "medium_task", "duration": round(duration, 3)}


@task
def slow_task() -> dict:
    """A task that takes longer to complete.

    Returns:
        A dict with task name and duration.
    """
    start = time.monotonic()
    time.sleep(0.1)
    duration = time.monotonic() - start
    print(f"slow_task completed in {duration:.3f}s")
    return {"task": "slow_task", "duration": round(duration, 3)}


@task
def sla_report(results: list[dict], thresholds: dict | None = None) -> str:
    """Check task durations against SLA thresholds and report breaches.

    Args:
        results: A list of task result dicts with name and duration.
        thresholds: A dict mapping task names to max allowed seconds.

    Returns:
        A formatted SLA report string.
    """
    if thresholds is None:
        thresholds = {"fast_task": 0.5, "medium_task": 0.5, "slow_task": 0.5}

    lines = ["SLA Report:"]
    breaches = 0
    for result in results:
        name = result["task"]
        duration = result["duration"]
        limit = thresholds.get(name, 1.0)
        status = "OK" if duration <= limit else "BREACH"
        if status == "BREACH":
            breaches += 1
        lines.append(f"  {name}: {duration:.3f}s (limit={limit}s) [{status}]")

    lines.append(f"Total breaches: {breaches}/{len(results)}")
    report = "\n".join(lines)
    print(report)
    return report


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="048_sla_monitoring", log_prints=True)
def sla_monitoring_flow() -> None:
    """Run tasks and check their durations against SLA thresholds."""
    r1 = fast_task()
    r2 = medium_task()
    r3 = slow_task()
    sla_report([r1, r2, r3])


if __name__ == "__main__":
    sla_monitoring_flow()
