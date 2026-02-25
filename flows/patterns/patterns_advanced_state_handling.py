"""Advanced State Handling.

Use allow_failure and return_state to handle mixed task outcomes.

Airflow equivalent: Trigger rules (all_success, all_done, etc.) (DAG 007).
Prefect approach:    allow_failure, return_state=True, state inspection.
"""

from typing import Any

from dotenv import load_dotenv
from prefect import allow_failure, flow, task

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def succeed_task() -> str:
    """A task that always succeeds.

    Returns:
        A success message.
    """
    msg = "Task succeeded"
    print(msg)
    return msg


@task
def fail_task() -> str:
    """A task that always fails.

    Raises:
        ValueError: Always.
    """
    raise ValueError("Intentional failure for demonstration")


@task
def skip_task(should_skip: bool = True) -> str:
    """A task that conditionally skips its work.

    Args:
        should_skip: Whether to skip the task.

    Returns:
        A message indicating what happened.
    """
    if should_skip:
        print("Skipping task")
        return "skipped"
    print("Running task")
    return "completed"


@task
def inspect_states(states: list[dict[str, Any]]) -> str:
    """Inspect upstream task states and summarize outcomes.

    Args:
        states: A list of dicts describing upstream outcomes.

    Returns:
        A summary of task states.
    """
    summary = f"Upstream results: {len(states)} tasks inspected"
    for s in states:
        summary += f"\n  - {s['name']}: {s['status']}"
    print(summary)
    return summary


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="patterns_advanced_state_handling", log_prints=True)
def advanced_state_handling_flow() -> None:
    """Demonstrate advanced state handling with allow_failure."""
    # Run a successful task
    succeed_task()

    # Run a failing task with allow_failure
    fail_future = fail_task.submit()

    # Run a task that depends on the failing task via allow_failure
    skip_result = skip_task(wait_for=[allow_failure(fail_future)])

    # Inspect states
    state_info = [
        {"name": "succeed_task", "status": "completed"},
        {"name": "fail_task", "status": "failed"},
        {"name": "skip_task", "status": skip_result},
    ]
    inspect_states(state_info)


if __name__ == "__main__":
    load_dotenv()
    advanced_state_handling_flow()
