"""Reusable task library for prefect-examples.

Import these tasks into any flow to avoid code duplication.
"""

from prefect import task


@task
def print_message(msg: str) -> str:
    """Print a message and return it."""
    print(msg)
    return msg


@task
def square_number(n: int) -> int:
    """Return n squared."""
    return n * n
