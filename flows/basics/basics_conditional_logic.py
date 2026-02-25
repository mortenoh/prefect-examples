"""Conditional Logic.

Branch flow execution using plain Python control flow.

Airflow equivalent: BranchPythonOperator.
Prefect approach:    native Python if/elif/else.
"""

from dotenv import load_dotenv
from prefect import flow, task


@task
def check_condition() -> str:
    """Evaluate a condition and return the chosen branch.

    Returns:
        A branch identifier: "a", "b", or anything else for the default.
    """
    result = "a"
    print(f"Condition evaluated to: {result!r}")
    return result


@task
def path_a() -> str:
    """Execute branch A logic."""
    msg = "Took path A"
    print(msg)
    return msg


@task
def path_b() -> str:
    """Execute branch B logic."""
    msg = "Took path B"
    print(msg)
    return msg


@task
def default_path() -> str:
    """Execute the default/fallback branch."""
    msg = "Took the default path"
    print(msg)
    return msg


@flow(name="basics_conditional_logic", log_prints=True)
def conditional_logic_flow() -> None:
    """Branch based on the return value of check_condition."""
    branch = check_condition()

    if branch == "a":
        path_a()
    elif branch == "b":
        path_b()
    else:
        default_path()


if __name__ == "__main__":
    load_dotenv()
    conditional_logic_flow()
