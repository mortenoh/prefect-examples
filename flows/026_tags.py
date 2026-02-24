"""026 — Tags.

Demonstrate tagging tasks and flows for organisation and filtering.

Airflow equivalent: DAG/task tags for filtering in the UI.
Prefect approach:    tags= on @task, tags() context manager for runtime tagging.
"""

from prefect import flow, tags, task


@task(tags=["etl", "extract"])
def extract_sales() -> list[dict]:
    """Extract sales records with ETL extract tags.

    Returns:
        A list of sales record dicts.
    """
    records = [
        {"id": 1, "product": "Widget A", "amount": 29.99},
        {"id": 2, "product": "Widget B", "amount": 49.99},
        {"id": 3, "product": "Gadget C", "amount": 19.99},
    ]
    print(f"Extracted {len(records)} sales records")
    return records


@task(tags=["etl", "transform"])
def transform_sales(records: list[dict]) -> list[dict]:
    """Apply transformations to sales records.

    Args:
        records: Raw sales records.

    Returns:
        Transformed records with added tax field.
    """
    transformed = [{**r, "tax": round(r["amount"] * 0.1, 2)} for r in records]
    print(f"Transformed {len(transformed)} records")
    return transformed


@task
def generic_task(data: str) -> str:
    """A task that receives runtime tags from the tags() context manager.

    Args:
        data: Input data string.

    Returns:
        A confirmation message.
    """
    msg = f"generic_task processed: {data}"
    print(msg)
    return msg


@flow(name="026_tags", log_prints=True)
def tags_flow() -> None:
    """Demonstrate static task tags and runtime tags context manager."""
    # Flow-level tags via context manager
    with tags("examples", "phase-2"):
        # Tasks with static tags
        records = extract_sales()
        transform_sales(records)

    # Runtime tags via context manager
    with tags("ad-hoc", "debug"):
        generic_task("debug-data-1")
        generic_task("debug-data-2")


if __name__ == "__main__":
    tags_flow()
