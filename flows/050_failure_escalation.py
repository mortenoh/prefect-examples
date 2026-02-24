"""050 -- Failure Escalation.

Progressive escalation: retry -> warn -> alert on persistent failures.

Airflow equivalent: Progressive retry with escalating callbacks (DAG 075).
Prefect approach:    retries with on_failure hooks for escalation logging.
"""

from prefect import flow, task

# ---------------------------------------------------------------------------
# State tracking
# ---------------------------------------------------------------------------

_attempt_counter: dict[str, int] = {}


# ---------------------------------------------------------------------------
# Hooks
# ---------------------------------------------------------------------------


def on_task_failure(task, task_run, state):
    """Log escalation when a task fails.

    Args:
        task: The task object.
        task_run: The task-run metadata.
        state: The failed state.
    """
    print(f"HOOK  WARNING: Task {task_run.name!r} failed — escalating")


def on_flow_completion(flow, flow_run, state):
    """Log recovery or final outcome when the flow completes.

    Args:
        flow: The flow object.
        flow_run: The flow-run metadata.
        state: The final state.
    """
    print(f"HOOK  Flow {flow_run.name!r} finished with state: {state.name}")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(retries=3, retry_delay_seconds=0, on_failure=[on_task_failure])
def flaky_task(fail_count: int = 2) -> str:
    """A task that fails a configurable number of times before succeeding.

    Args:
        fail_count: How many times to fail before succeeding.

    Returns:
        A success message with the attempt number.
    """
    key = "flaky_task_050"
    _attempt_counter[key] = _attempt_counter.get(key, 0) + 1
    attempt = _attempt_counter[key]

    if attempt <= fail_count:
        raise ValueError(f"Attempt {attempt}/{fail_count} -- simulated failure")

    msg = f"flaky_task succeeded on attempt {attempt}"
    print(msg)
    return msg


@task
def stable_task() -> str:
    """A task that always succeeds.

    Returns:
        A success message.
    """
    msg = "stable_task completed"
    print(msg)
    return msg


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="050_failure_escalation", log_prints=True, on_completion=[on_flow_completion])
def failure_escalation_flow() -> None:
    """Demonstrate failure escalation with retries and hooks."""
    _attempt_counter.clear()
    stable_task()
    flaky_task(fail_count=2)


if __name__ == "__main__":
    failure_escalation_flow()
