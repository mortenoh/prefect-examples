"""003 — Task Dependencies.

Chain tasks with explicit data dependencies and parallel execution.

Airflow equivalent: >> operator / set_downstream.
Prefect approach:    return-value wiring; .submit() for parallelism.
"""

from prefect import flow, task


@task
def start() -> str:
    """Kick off the pipeline and return an initial value."""
    msg = "started"
    print(f"start -> {msg}")
    return msg


@task
def task_a(upstream: str) -> str:
    """Process branch A."""
    msg = f"a({upstream})"
    print(f"task_a -> {msg}")
    return msg


@task
def task_b(upstream: str) -> str:
    """Process branch B."""
    msg = f"b({upstream})"
    print(f"task_b -> {msg}")
    return msg


@task
def task_c(upstream: str) -> str:
    """Process branch C."""
    msg = f"c({upstream})"
    print(f"task_c -> {msg}")
    return msg


@task
def join(results: list[str]) -> str:
    """Collect parallel results into a single summary."""
    summary = " | ".join(results)
    print(f"join -> {summary}")
    return summary


@flow(name="003_task_dependencies", log_prints=True)
def task_dependencies_flow() -> None:
    """Run start, fan out to a/b/c in parallel, then join."""
    initial = start()

    future_a = task_a.submit(initial)
    future_b = task_b.submit(initial)
    future_c = task_c.submit(initial)

    join([future_a.result(), future_b.result(), future_c.result()])


if __name__ == "__main__":
    task_dependencies_flow()
