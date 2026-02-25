"""Dynamic Tasks.

Fan out work dynamically over a list of items.

Airflow equivalent: dynamic task mapping (expand()).
Prefect approach:    .map() for dynamic fan-out.
"""

from prefect import flow, task


@task
def generate_items() -> list[str]:
    """Return a list of items to process."""
    items = ["item-a", "item-b", "item-c", "item-d"]
    print(f"Generated {len(items)} items")
    return items


@task
def process_item(item: str) -> str:
    """Process a single item and return the result."""
    result = f"processed({item})"
    print(result)
    return result


@task
def summarize(results: list[str]) -> str:
    """Combine processed results into a summary."""
    summary = " | ".join(results)
    print(f"Summary: {summary}")
    return summary


@flow(name="basics_dynamic_tasks", log_prints=True)
def dynamic_tasks_flow() -> None:
    """Generate items, map processing over them, then summarize."""
    items = generate_items()
    futures = process_item.map(items)
    summarize([f.result() for f in futures])


if __name__ == "__main__":
    dynamic_tasks_flow()
