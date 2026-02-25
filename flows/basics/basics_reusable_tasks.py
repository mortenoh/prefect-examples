"""Reusable Tasks.

Import shared tasks from the project's task library.

Airflow equivalent: custom operators / shared utils.
Prefect approach:    regular Python imports — tasks are just functions.
"""

from dotenv import load_dotenv
from prefect import flow

from prefect_examples.tasks import print_message, square_number


@flow(name="basics_reusable_tasks", log_prints=True)
def reusable_tasks_flow() -> None:
    """Use tasks defined in the shared library."""
    print_message("Hello from reusable tasks!")
    result = square_number(7)
    print(f"7 squared = {result}")


if __name__ == "__main__":
    load_dotenv()
    reusable_tasks_flow()
