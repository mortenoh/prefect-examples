"""Polling Tasks.

Wait for an external condition to become true before proceeding.

Airflow equivalent: Sensor (poke/reschedule).
Prefect approach:    while-loop polling with time.sleep().
"""

import time

from prefect import flow, task


@task
def poll_condition(
    name: str,
    interval: float = 1.0,
    timeout: float = 10.0,
    succeed_after: float = 3.0,
) -> str:
    """Poll until a condition is met or the timeout is exceeded.

    Args:
        name: Identifier for this polling task.
        interval: Seconds between poll attempts.
        timeout: Maximum seconds to wait before raising TimeoutError.
        succeed_after: Seconds after which the condition succeeds.

    Returns:
        A message confirming the condition was met.

    Raises:
        TimeoutError: If the timeout is reached before the condition is met.
    """
    start_time = time.monotonic()
    while True:
        elapsed = time.monotonic() - start_time
        if elapsed >= succeed_after:
            msg = f"[{name}] Condition met after {elapsed:.1f}s"
            print(msg)
            return msg
        if elapsed >= timeout:
            raise TimeoutError(f"[{name}] Timed out after {elapsed:.1f}s")
        print(f"[{name}] Waiting… elapsed={elapsed:.1f}s")
        time.sleep(interval)


@task
def process_after_poll(results: list[str]) -> str:
    """Process the results collected from polling tasks."""
    summary = " & ".join(results)
    print(f"Post-poll processing: {summary}")
    return summary


@flow(name="basics_polling_tasks", log_prints=True)
def polling_flow() -> None:
    """Launch two polls in parallel, then process their results."""
    future_a = poll_condition.submit(name="sensor-A", succeed_after=2.0)
    future_b = poll_condition.submit(name="sensor-B", succeed_after=4.0)
    process_after_poll([future_a.result(), future_b.result()])


if __name__ == "__main__":
    polling_flow()
