"""Transactions.

Use Prefect transactions for atomic task groups with rollback on failure.

Airflow equivalent: No direct equivalent -- Prefect-specific feature.
Prefect approach:    transaction() context manager for atomic operations.
"""

from prefect import flow, task
from prefect.transactions import transaction

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def step_a() -> str:
    """First step in the transaction.

    Returns:
        A result string.
    """
    msg = "step_a completed"
    print(msg)
    return msg


@task
def step_b() -> str:
    """Second step in the transaction.

    Returns:
        A result string.
    """
    msg = "step_b completed"
    print(msg)
    return msg


@task
def step_c() -> str:
    """Third step in the transaction.

    Returns:
        A result string.
    """
    msg = "step_c completed"
    print(msg)
    return msg


@task
def summarize_transaction(results: list[str]) -> str:
    """Summarize the transaction outcome.

    Args:
        results: List of step results.

    Returns:
        A summary string.
    """
    summary = f"Transaction completed: {len(results)} steps"
    print(summary)
    return summary


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="patterns_transactions", log_prints=True)
def transactions_flow() -> None:
    """Demonstrate atomic task execution with transactions."""
    with transaction():
        a = step_a()
        b = step_b()
        c = step_c()
    summarize_transaction([a, b, c])


if __name__ == "__main__":
    transactions_flow()
