"""Early Return.

Short-circuit a flow with a plain Python return statement.

Airflow equivalent: ShortCircuitOperator.
Prefect approach:    Python return statement.
"""

from dotenv import load_dotenv
from prefect import flow, task


@task
def should_continue() -> bool:
    """Decide whether the flow should proceed.

    Returns:
        True if work should continue, False otherwise.
    """
    result = True
    print(f"should_continue -> {result}")
    return result


@task
def do_work() -> str:
    """Perform the primary unit of work.

    Returns:
        A confirmation message.
    """
    msg = "Work done"
    print(msg)
    return msg


@task
def do_more_work() -> str:
    """Perform additional follow-up work.

    Returns:
        A confirmation message.
    """
    msg = "More work done"
    print(msg)
    return msg


@flow(name="basics_early_return", log_prints=True)
def early_return_flow(skip: bool = False) -> None:
    """Demonstrate early return to short-circuit execution.

    Args:
        skip: If True, return immediately without doing any work.
    """
    if skip:
        print("Skip flag is set — returning early")
        return

    proceed = should_continue()
    if not proceed:
        print("should_continue returned False — returning early")
        return

    do_work()
    do_more_work()
    print("All work completed")


if __name__ == "__main__":
    load_dotenv()
    early_return_flow()
