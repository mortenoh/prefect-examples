"""007 — State Handlers.

React to task/flow state changes with hook functions.

Airflow equivalent: on_failure_callback / trigger_rule.
Prefect approach:    state hooks + allow_failure.
"""

from prefect import allow_failure, flow, task

# ---------------------------------------------------------------------------
# Hooks (plain functions, NOT tasks)
# ---------------------------------------------------------------------------


def on_task_failure(task, task_run, state):
    """Log details when a task enters a failed state.

    Args:
        task: The task object that failed.
        task_run: The task-run metadata.
        state: The final state of the task run.
    """
    print(f"HOOK  Task {task_run.name!r} failed with state: {state.name}")


def on_flow_completion(flow, flow_run, state):
    """Log details when the flow completes (regardless of outcome).

    Args:
        flow: The flow object.
        flow_run: The flow-run metadata.
        state: The final state of the flow run.
    """
    print(f"HOOK  Flow {flow_run.name!r} completed with state: {state.name}")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def succeed_task() -> str:
    """A task that always succeeds."""
    msg = "Success!"
    print(msg)
    return msg


@task(on_failure=[on_task_failure])
def fail_task():
    """A task that always raises an error."""
    raise ValueError("Intentional failure for demonstration")


@task
def always_run_task() -> str:
    """A task that should run regardless of upstream failures."""
    msg = "I run no matter what."
    print(msg)
    return msg


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="007_state_handlers", log_prints=True, on_completion=[on_flow_completion])
def state_handlers_flow() -> None:
    """Demonstrate state hooks and allow_failure."""
    succeed_task()
    failing_future = fail_task.submit()
    always_run_task(wait_for=[allow_failure(failing_future)])


if __name__ == "__main__":
    state_handlers_flow()
