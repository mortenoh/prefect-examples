"""025 — Structured Logging.

Demonstrate Prefect's structured logging with get_run_logger(),
print-based logging, and extra context fields.

Airflow equivalent: Python logging in operators, task instance logger.
Prefect approach:    get_run_logger() for structured logs, log_prints=True
                     for print() capture, extra dict for context fields.
"""

from prefect import flow, get_run_logger, task


@task
def task_with_logger(item: str) -> str:
    """Process an item using the Prefect run logger.

    get_run_logger() returns a logger bound to the current task run.
    Outside Prefect runtime, it falls back to a standard library logger.

    Args:
        item: The item to process.

    Returns:
        A processing result string.
    """
    logger = get_run_logger()
    logger.info("Starting processing of %s", item)
    result = f"processed:{item}"
    logger.info("Finished processing: %s", result)
    return result


@task
def task_with_print(item: str) -> str:
    """Process an item using print statements.

    With log_prints=True on the flow, print() output is captured
    as INFO-level log entries.

    Args:
        item: The item to process.

    Returns:
        A processing result string.
    """
    print(f"Processing item: {item}")
    result = f"printed:{item}"
    print(f"Result: {result}")
    return result


@task
def task_with_extra_context(user: str, action: str) -> str:
    """Log with extra context fields for structured observability.

    The extra dict is included in log records and can be consumed
    by log aggregation tools.

    Args:
        user: The user performing the action.
        action: The action being performed.

    Returns:
        A message describing the action.
    """
    logger = get_run_logger()
    msg = f"User {user} performed {action}"
    logger.info(msg, extra={"user": user, "action": action})
    return msg


@flow(name="025_structured_logging", log_prints=True)
def structured_logging_flow() -> None:
    """Demonstrate three approaches to logging in Prefect tasks."""
    task_with_logger("alpha")
    task_with_print("beta")
    task_with_extra_context("alice", "deploy")


if __name__ == "__main__":
    structured_logging_flow()
