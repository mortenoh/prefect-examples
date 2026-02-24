"""024 — Advanced Retries.

Demonstrate advanced retry configuration: exponential backoff,
jitter, and conditional retry logic.

Airflow equivalent: Custom retry logic in operators, exponential_backoff.
Prefect approach:    retry_delay_seconds (list), retry_jitter_factor,
                     retry_condition_fn on @task.

Builds on: Flow 012 (basic retries).
"""

from prefect import flow, task

_attempt_counters: dict[str, int] = {}


def retry_on_value_error(task, task_run, state) -> bool:  # type: ignore[no-untyped-def]
    """Only retry if the exception is a ValueError.

    Args:
        task: The task object.
        task_run: The task run object.
        state: The current state of the task run.

    Returns:
        True if the task should be retried, False otherwise.
    """
    exc = state.result(raise_on_failure=False)
    if isinstance(exc, BaseException):
        return isinstance(exc, ValueError)
    return False


@task(retries=3, retry_delay_seconds=[1, 2, 4])
def backoff_task(fail_count: int = 2) -> str:
    """Fail a configurable number of times with exponential backoff.

    Retry delays: 1s, 2s, 4s — doubling each attempt.

    Args:
        fail_count: How many initial attempts should fail.

    Returns:
        A success message once attempts exceed fail_count.

    Raises:
        RuntimeError: On each attempt until the threshold is passed.
    """
    key = "backoff_task"
    _attempt_counters[key] = _attempt_counters.get(key, 0) + 1
    attempt = _attempt_counters[key]
    if attempt <= fail_count:
        raise RuntimeError(f"Attempt {attempt}/{fail_count} — backoff failure")
    msg = f"backoff_task succeeded on attempt {attempt}"
    print(msg)
    return msg


@task(retries=2, retry_delay_seconds=1, retry_jitter_factor=0.5)
def jittery_task(fail_count: int = 1) -> str:
    """Fail once then succeed, with jitter added to the retry delay.

    Jitter adds randomness (±50%) to the delay to prevent thundering
    herd problems when many tasks retry simultaneously.

    Args:
        fail_count: How many initial attempts should fail.

    Returns:
        A success message once attempts exceed fail_count.

    Raises:
        RuntimeError: On each attempt until the threshold is passed.
    """
    key = "jittery_task"
    _attempt_counters[key] = _attempt_counters.get(key, 0) + 1
    attempt = _attempt_counters[key]
    if attempt <= fail_count:
        raise RuntimeError(f"Attempt {attempt}/{fail_count} — jittery failure")
    msg = f"jittery_task succeeded on attempt {attempt}"
    print(msg)
    return msg


@task(retries=2, retry_delay_seconds=1, retry_condition_fn=retry_on_value_error)
def conditional_retry_task(error_type: str) -> str:
    """Retry only on ValueErrors, not on other exceptions.

    Args:
        error_type: The type of error to raise ("value", "type", or "none").

    Returns:
        A success message if no error is raised.

    Raises:
        ValueError: If error_type is "value".
        TypeError: If error_type is "type".
    """
    if error_type == "value":
        raise ValueError("Simulated ValueError — will retry")
    if error_type == "type":
        raise TypeError("Simulated TypeError — will NOT retry")
    msg = f"conditional_retry_task completed (error_type={error_type!r})"
    print(msg)
    return msg


@flow(name="024_advanced_retries", log_prints=True)
def advanced_retries_flow() -> None:
    """Demonstrate advanced retry patterns: backoff, jitter, conditional."""
    # Reset counters for a clean run
    _attempt_counters.clear()

    backoff_task()
    jittery_task()
    conditional_retry_task("none")


if __name__ == "__main__":
    advanced_retries_flow()
