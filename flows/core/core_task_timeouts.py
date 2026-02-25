"""Task Timeouts.

Demonstrate task-level and flow-level timeout configuration.

Airflow equivalent: execution_timeout on operators.
Prefect approach:    timeout_seconds on @task and @flow decorators.
"""

import time

from dotenv import load_dotenv
from prefect import flow, task


@task(timeout_seconds=3)
def quick_task() -> str:
    """A task that completes well within its timeout.

    Returns:
        A success message.
    """
    msg = "quick_task completed in time"
    print(msg)
    return msg


@task(timeout_seconds=2)
def slow_task() -> str:
    """A task that exceeds its timeout.

    The 10-second sleep will be interrupted by the 2-second timeout
    when run in Prefect runtime. When called via .fn(), the timeout
    is bypassed and the full sleep executes.

    Returns:
        A success message (only reached if timeout is bypassed).
    """
    print("slow_task starting — will sleep 10s")
    time.sleep(10)
    msg = "slow_task completed"
    print(msg)
    return msg


@task
def cleanup_task(timed_out: bool) -> str:
    """Run cleanup after a potential timeout.

    Args:
        timed_out: Whether the preceding task timed out.

    Returns:
        A message describing the cleanup action taken.
    """
    msg = "cleanup_task: recovering from timeout" if timed_out else "cleanup_task: normal cleanup"
    print(msg)
    return msg


@flow(name="core_task_timeouts", log_prints=True, timeout_seconds=30)
def task_timeouts_flow() -> None:
    """Run tasks with timeouts and handle timeout failures gracefully."""
    quick_task()

    timed_out = False
    try:
        slow_task()
    except Exception:
        print("slow_task timed out — running cleanup")
        timed_out = True

    cleanup_task(timed_out)


if __name__ == "__main__":
    load_dotenv()
    task_timeouts_flow()
