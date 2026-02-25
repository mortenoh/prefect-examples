"""Cross-Dataset Validation.

Validates referential integrity between related datasets: orders must
reference existing customers and products. Detects and reports orphan records.

Airflow equivalent: Referential integrity checks (DAG 071).
Prefect approach:    Pure @task functions for FK checks, Pydantic models for
                     structured results.
"""

from typing import Any

from dotenv import load_dotenv
from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class IntegrityResult(BaseModel):
    """Result of a referential integrity check."""

    check_name: str
    child_key: str
    parent_key: str
    total_children: int
    orphan_count: int
    orphan_keys: list[Any]
    passed: bool


class CrossValidationReport(BaseModel):
    """Summary of all cross-dataset validation checks."""

    checks_run: int
    checks_passed: int
    total_orphans: int
    passed: bool
    results: list[IntegrityResult]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def generate_customers() -> list[dict[str, Any]]:
    """Generate a customer dataset.

    Returns:
        List of customer dicts.
    """
    return [{"customer_id": i, "name": f"Customer {i}", "region": ["US", "EU", "APAC"][i % 3]} for i in range(1, 11)]


@task
def generate_products() -> list[dict[str, Any]]:
    """Generate a product dataset.

    Returns:
        List of product dicts.
    """
    return [{"product_id": i, "name": f"Product {i}", "price": 10.0 + i * 5} for i in range(1, 8)]


@task
def generate_orders() -> list[dict[str, Any]]:
    """Generate an order dataset with some deliberately orphaned references.

    Returns:
        List of order dicts (some with invalid customer/product IDs).
    """
    orders = []
    for i in range(1, 16):
        orders.append(
            {
                "order_id": i,
                "customer_id": i if i <= 10 else 99,
                "product_id": (i % 7) + 1 if i <= 12 else 50,
                "quantity": i % 5 + 1,
            }
        )
    return orders


@task
def check_referential_integrity(
    child_data: list[dict[str, Any]],
    parent_data: list[dict[str, Any]],
    child_key: str,
    parent_key: str,
    check_name: str,
) -> IntegrityResult:
    """Check that all child key values exist in the parent dataset.

    Args:
        child_data: The child (referencing) dataset.
        parent_data: The parent (referenced) dataset.
        child_key: Column name in the child dataset.
        parent_key: Column name in the parent dataset.
        check_name: Name for this check.

    Returns:
        IntegrityResult with orphan details.
    """
    parent_values = {row[parent_key] for row in parent_data}
    orphan_keys = []
    for row in child_data:
        if row[child_key] not in parent_values:
            orphan_keys.append(row[child_key])
    unique_orphans = sorted(set(orphan_keys))
    passed = len(orphan_keys) == 0
    print(f"{check_name}: {'PASS' if passed else 'FAIL'} ({len(orphan_keys)} orphans)")
    return IntegrityResult(
        check_name=check_name,
        child_key=child_key,
        parent_key=parent_key,
        total_children=len(child_data),
        orphan_count=len(orphan_keys),
        orphan_keys=unique_orphans,
        passed=passed,
    )


@task
def cross_validation_report(results: list[IntegrityResult]) -> CrossValidationReport:
    """Build a summary report from integrity check results.

    Args:
        results: List of individual check results.

    Returns:
        Cross-validation report.
    """
    total_orphans = sum(r.orphan_count for r in results)
    checks_passed = sum(1 for r in results if r.passed)
    return CrossValidationReport(
        checks_run=len(results),
        checks_passed=checks_passed,
        total_orphans=total_orphans,
        passed=all(r.passed for r in results),
        results=results,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="data_engineering_cross_dataset_validation", log_prints=True)
def cross_dataset_validation_flow() -> CrossValidationReport:
    """Validate referential integrity across orders, customers, and products.

    Returns:
        Cross-validation report.
    """
    customers = generate_customers()
    products = generate_products()
    orders = generate_orders()

    check_customer = check_referential_integrity(
        orders,
        customers,
        "customer_id",
        "customer_id",
        "orders_to_customers",
    )
    check_product = check_referential_integrity(
        orders,
        products,
        "product_id",
        "product_id",
        "orders_to_products",
    )

    report = cross_validation_report([check_customer, check_product])
    print(f"Cross-validation: {report.checks_passed}/{report.checks_run} passed, {report.total_orphans} total orphans")
    return report


if __name__ == "__main__":
    load_dotenv()
    cross_dataset_validation_flow()
