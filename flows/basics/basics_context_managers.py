"""Context Managers.

Guarantee resource setup and teardown using Python patterns.

Airflow equivalent: @setup / @teardown decorators.
Prefect approach:    Python context managers and try/finally.
"""

from collections.abc import Iterator
from contextlib import contextmanager

from prefect import flow, task


@task
def setup_resource() -> str:
    """Acquire a shared resource (simulated).

    Returns:
        A resource identifier string.
    """
    resource = "db-connection-42"
    print(f"Setup resource: {resource}")
    return resource


@task
def use_resource(resource: str) -> str:
    """Perform work using the acquired resource.

    Args:
        resource: The resource identifier to use.

    Returns:
        A result string describing the operation.
    """
    result = f"queried 150 rows via {resource}"
    print(f"Used resource: {result}")
    return result


@task
def cleanup_resource(resource: str) -> None:
    """Release the resource (simulated).

    Args:
        resource: The resource identifier to clean up.
    """
    print(f"Cleaned up resource: {resource}")


@contextmanager
def managed_resource() -> Iterator[str]:
    """Context manager that sets up and tears down a resource.

    Yields:
        The resource identifier string.
    """
    resource = "db-connection-42"
    print(f"[ctx] Setup resource: {resource}")
    try:
        yield resource
    finally:
        print(f"[ctx] Cleaned up resource: {resource}")


@flow(name="basics_context_managers", log_prints=True)
def context_managers_flow() -> None:
    """Demonstrate try/finally cleanup around task-managed resources."""
    resource = setup_resource()
    try:
        use_resource(resource)
    finally:
        cleanup_resource(resource)


if __name__ == "__main__":
    context_managers_flow()
