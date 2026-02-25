"""Runtime Context.

Access flow run and task run metadata at runtime.

Airflow equivalent: Jinja templating, macros, runtime info (DAGs 008, 046, 052).
Prefect approach:    prefect.runtime for flow_run and task_run metadata.
"""

from typing import Any

from dotenv import load_dotenv
from prefect import flow, task
from prefect.runtime import flow_run, task_run

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def get_task_info() -> dict[str, Any]:
    """Read task run context information.

    Returns:
        A dict with task run metadata (or defaults if outside runtime).
    """
    info = {
        "task_run_name": task_run.name or "unknown",
        "task_run_id": str(task_run.id) if task_run.id else "unknown",
    }
    print(f"Task info: {info}")
    return info


@task
def get_flow_info() -> dict[str, Any]:
    """Read flow run context information.

    Returns:
        A dict with flow run metadata (or defaults if outside runtime).
    """
    info = {
        "flow_run_name": flow_run.name or "unknown",
        "flow_run_id": str(flow_run.id) if flow_run.id else "unknown",
        "flow_name": flow_run.flow_name or "unknown",
    }
    print(f"Flow info: {info}")
    return info


@task
def get_parameters() -> dict[str, Any]:
    """Read flow-level parameters from runtime context.

    Returns:
        A dict with the flow parameters (or empty dict if outside runtime).
    """
    params = flow_run.parameters or {}
    print(f"Flow parameters: {params}")
    return dict(params)


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="patterns_runtime_context", log_prints=True)
def runtime_context_flow(env: str = "dev", batch_size: int = 100) -> None:
    """Demonstrate reading runtime context in tasks.

    Args:
        env: The environment label.
        batch_size: The batch size parameter.
    """
    flow_info = get_flow_info()
    get_task_info()
    params = get_parameters()
    print(f"Flow: {flow_info['flow_name']}, env={env}, batch_size={batch_size}")
    print(f"Parameters from context: {params}")


if __name__ == "__main__":
    load_dotenv()
    runtime_context_flow()
