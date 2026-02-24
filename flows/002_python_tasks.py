"""002 — Python Tasks.

Simple tasks with typed parameters and return values.

Airflow equivalent: PythonOperator with python_callable.
Prefect approach:    @task decorator — any Python function becomes a task.
"""

from prefect import flow, task


@task
def greet(name: str, greeting: str = "Hello") -> str:
    """Build and return a greeting string."""
    msg = f"{greeting}, {name}!"
    print(msg)
    return msg


@task
def compute_sum(a: int, b: int) -> int:
    """Return the sum of two integers."""
    result = a + b
    print(f"{a} + {b} = {result}")
    return result


@flow(name="002_python_tasks", log_prints=True)
def python_tasks_flow() -> None:
    """Demonstrate basic @task usage with typed params."""
    greet("World")
    compute_sum(3, 7)


if __name__ == "__main__":
    python_tasks_flow()
