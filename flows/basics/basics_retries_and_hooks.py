"""Retries and Hooks.

Demonstrate automatic retries and lifecycle hooks on tasks and flows.

Airflow equivalent: retries + on_failure_callback.
Prefect approach:    retries/retry_delay_seconds on @task; state hooks.
"""

from typing import Any

from prefect import flow, task

_attempt_counter: dict[str, int] = {}


def my_task_failure_hook(task: Any, task_run: Any, state: Any) -> None:
    """Log information when a task enters a failed state."""
    print(f"HOOK — task '{task_run.name}' failed with state: {state.name}")


def my_flow_completion_hook(flow: Any, flow_run: Any, state: Any) -> None:
    """Log information when a flow run completes (success or failure)."""
    print(f"HOOK — flow '{flow_run.name}' completed with state: {state.name}")


@task(retries=3, retry_delay_seconds=1, on_failure=[my_task_failure_hook])
def flaky_task(fail_count: int = 2) -> str:
    """Fail a configurable number of times before succeeding.

    Args:
        fail_count: How many initial attempts should fail.

    Returns:
        A success message once attempts exceed fail_count.

    Raises:
        ValueError: On each attempt until the threshold is passed.
    """
    key = "flaky_task"
    _attempt_counter[key] = _attempt_counter.get(key, 0) + 1
    attempt = _attempt_counter[key]
    if attempt <= fail_count:
        raise ValueError(f"Attempt {attempt}/{fail_count} — simulated failure")
    msg = f"flaky_task succeeded on attempt {attempt}"
    print(msg)
    return msg


@task
def reliable_task() -> str:
    """A task that always succeeds."""
    msg = "reliable_task completed"
    print(msg)
    return msg


@flow(name="basics_retries_and_hooks", log_prints=True, on_completion=[my_flow_completion_hook])
def retries_flow() -> None:
    """Run a flaky task (with retries) followed by a reliable task."""
    flaky_task()
    reliable_task()


if __name__ == "__main__":
    retries_flow()
