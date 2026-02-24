"""039 — Work Pools.

Demonstrate work pool concepts for production deployments.

Airflow equivalent: Executors (Local, Celery, Kubernetes).
Prefect approach:    Work pools define WHERE work runs. flow.deploy()
                     targets a named pool. Workers poll for scheduled runs.

Key concepts:
    - Work pool types: process, docker, kubernetes
    - Workers: long-running processes that poll a work pool
    - flow.deploy() vs flow.serve():
        - serve() runs in-process (simple, no infra needed)
        - deploy() sends runs to a work pool (production-grade)
    - CLI: prefect work-pool create "my-pool" --type process

    # Example deployment (not auto-executed):
    # flow_fn.deploy(
    #     name="039-work-pool",
    #     work_pool_name="my-pool",
    # )
"""

from prefect import flow, task


@task
def fetch_data(source: str) -> dict:
    """Fetch data from a named source.

    Args:
        source: The data source identifier.

    Returns:
        A dict with source data.
    """
    data = {"source": source, "records": [{"id": i, "value": i * 10} for i in range(1, 6)]}
    print(f"Fetched {len(data['records'])} records from {source}")
    return data


@task
def process_data(data: dict) -> str:
    """Process fetched data.

    Args:
        data: The source data dict.

    Returns:
        A processing summary string.
    """
    total = sum(r["value"] for r in data["records"])
    msg = f"Processed {len(data['records'])} records from {data['source']} (total={total})"
    print(msg)
    return msg


@task
def save_results(result: str) -> str:
    """Save processing results.

    Args:
        result: The processing summary to save.

    Returns:
        A confirmation message.
    """
    msg = f"Saved: {result}"
    print(msg)
    return msg


@flow(name="039_work_pools", log_prints=True)
def work_pools_flow() -> None:
    """Standard pipeline suitable for work pool deployment.

    In production, deploy to a work pool:
        work_pools_flow.deploy(
            name="039-work-pool",
            work_pool_name="my-pool",
        )

    Then start a worker:
        prefect worker start --pool "my-pool"
    """
    data = fetch_data("production-db")
    result = process_data(data)
    save_results(result)


if __name__ == "__main__":
    # Run directly for local testing.
    # For production: deploy to a work pool (see docstring).
    work_pools_flow()
