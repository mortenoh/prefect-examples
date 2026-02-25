"""Reusable Utilities.

Build task utility decorators for consistent behaviour across flows.

Airflow equivalent: Custom hooks and sensors (DAG 056).
Prefect approach:    Python decorators that wrap @task with extra features.
"""

import functools
import time
from collections.abc import Callable
from typing import Any

from dotenv import load_dotenv
from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Utility decorators
# ---------------------------------------------------------------------------


def timed_task(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator that adds execution timing to a task function.

    Args:
        fn: The function to wrap with timing.

    Returns:
        A wrapped function that includes duration in its result.
    """

    @task(name=fn.__name__)
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start = time.monotonic()
        result = fn(*args, **kwargs)
        duration = round(time.monotonic() - start, 4)
        print(f"{fn.__name__} completed in {duration}s")
        if isinstance(result, dict):
            result["_duration"] = duration
        return result

    return wrapper


def validated_task(model: type[BaseModel]) -> Callable[..., Any]:
    """Decorator factory that validates task output against a Pydantic model.

    Args:
        model: The Pydantic model class to validate output against.

    Returns:
        A decorator that wraps a function as a validated task.
    """

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @task(name=fn.__name__)
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = fn(*args, **kwargs)
            validated = model(**result) if isinstance(result, dict) else result
            print(f"{fn.__name__} output validated against {model.__name__}")
            return validated

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class MetricResult(BaseModel):
    """A validated metric output."""

    name: str
    value: float
    unit: str


# ---------------------------------------------------------------------------
# Tasks using utilities
# ---------------------------------------------------------------------------


@timed_task
def compute_metric(name: str, value: float) -> dict[str, Any]:
    """Compute a named metric (wrapped with timing).

    Args:
        name: The metric name.
        value: The metric value.

    Returns:
        A dict with metric details.
    """
    time.sleep(0.01)
    return {"name": name, "value": value * 1.1, "unit": "ops/sec"}


@validated_task(MetricResult)
def produce_metric(name: str, value: float, unit: str) -> dict[str, Any]:
    """Produce a metric validated against MetricResult schema.

    Args:
        name: The metric name.
        value: The metric value.
        unit: The metric unit.

    Returns:
        A dict that will be validated as MetricResult.
    """
    return {"name": name, "value": value, "unit": unit}


@task
def report_metrics(metrics: list[Any]) -> str:
    """Produce a summary of collected metrics.

    Args:
        metrics: A list of metric results.

    Returns:
        A summary string.
    """
    summary = f"Collected {len(metrics)} metrics"
    print(summary)
    return summary


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="patterns_reusable_utilities", log_prints=True)
def reusable_utilities_flow() -> None:
    """Demonstrate reusable task utilities: timing and validation."""
    m1 = compute_metric("throughput", 1000.0)
    m2 = compute_metric("latency", 45.0)
    m3 = produce_metric("error_rate", 0.02, "percent")
    report_metrics([m1, m2, m3])


if __name__ == "__main__":
    load_dotenv()
    reusable_utilities_flow()
