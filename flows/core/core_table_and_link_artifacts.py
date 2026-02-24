"""Table and Link Artifacts.

Demonstrate table and link artifacts for structured data display.

Airflow equivalent: Custom UI plugins, external dashboards.
Prefect approach:    create_table_artifact() and create_link_artifact()
                     for structured data and reference links in the UI.
"""

from prefect import flow, task
from prefect.artifacts import create_link_artifact, create_table_artifact


@task
def compute_inventory() -> list[dict]:
    """Compute current inventory levels.

    Returns:
        A list of inventory records with item, quantity, and status.
    """
    inventory = [
        {"item": "Widget A", "quantity": 150, "status": "In Stock"},
        {"item": "Widget B", "quantity": 30, "status": "Low Stock"},
        {"item": "Gadget C", "quantity": 0, "status": "Out of Stock"},
        {"item": "Gadget D", "quantity": 200, "status": "In Stock"},
        {"item": "Part E", "quantity": 15, "status": "Low Stock"},
    ]
    print(f"Computed inventory for {len(inventory)} items")
    return inventory


@task
def publish_table(inventory: list[dict]) -> None:
    """Publish inventory data as a table artifact.

    Table artifacts render as formatted tables in the Prefect UI,
    making it easy to inspect structured data from flow runs.

    Args:
        inventory: List of inventory records.
    """
    create_table_artifact(
        key="inventory-snapshot",
        table=inventory,
        description="Current inventory levels",
    )
    print(f"Published table artifact with {len(inventory)} rows")


@task
def publish_links() -> None:
    """Publish reference links as link artifacts.

    Link artifacts provide quick access to related resources
    directly from the flow run page.
    """
    create_link_artifact(
        key="inventory-dashboard",
        link="https://example.com/dashboard/inventory",
        description="Live inventory dashboard",
    )
    create_link_artifact(
        key="inventory-docs",
        link="https://example.com/docs/inventory-api",
        description="Inventory API documentation",
    )
    print("Published 2 link artifacts")


@flow(name="core_table_and_link_artifacts", log_prints=True)
def table_and_link_artifacts_flow() -> None:
    """Compute inventory and publish table and link artifacts."""
    inventory = compute_inventory()
    publish_table(inventory)
    publish_links()


if __name__ == "__main__":
    table_and_link_artifacts_flow()
