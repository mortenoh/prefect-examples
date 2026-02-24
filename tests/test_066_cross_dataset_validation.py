"""Tests for flow 066 -- Cross-Dataset Validation."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_066",
    Path(__file__).resolve().parent.parent / "flows" / "066_cross_dataset_validation.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_066"] = _mod
_spec.loader.exec_module(_mod)

IntegrityResult = _mod.IntegrityResult
CrossValidationReport = _mod.CrossValidationReport
generate_customers = _mod.generate_customers
generate_products = _mod.generate_products
generate_orders = _mod.generate_orders
check_referential_integrity = _mod.check_referential_integrity
cross_validation_report = _mod.cross_validation_report
cross_dataset_validation_flow = _mod.cross_dataset_validation_flow


def test_integrity_result_model() -> None:
    r = IntegrityResult(
        check_name="test",
        child_key="fk",
        parent_key="pk",
        total_children=10,
        orphan_count=0,
        orphan_keys=[],
        passed=True,
    )
    assert r.passed is True


def test_generate_customers() -> None:
    customers = generate_customers.fn()
    assert len(customers) == 10
    assert "customer_id" in customers[0]


def test_generate_products() -> None:
    products = generate_products.fn()
    assert len(products) == 7


def test_generate_orders_has_orphans() -> None:
    orders = generate_orders.fn()
    assert len(orders) == 15
    customer_ids = {o["customer_id"] for o in orders}
    assert 99 in customer_ids


def test_check_integrity_pass() -> None:
    children = [{"fk": 1}, {"fk": 2}]
    parents = [{"pk": 1}, {"pk": 2}, {"pk": 3}]
    result = check_referential_integrity.fn(children, parents, "fk", "pk", "test")
    assert result.passed is True
    assert result.orphan_count == 0


def test_check_integrity_fail() -> None:
    children = [{"fk": 1}, {"fk": 99}]
    parents = [{"pk": 1}, {"pk": 2}]
    result = check_referential_integrity.fn(children, parents, "fk", "pk", "test")
    assert result.passed is False
    assert result.orphan_count == 1
    assert 99 in result.orphan_keys


def test_flow_runs() -> None:
    state = cross_dataset_validation_flow(return_state=True)
    assert state.is_completed()


def test_flow_detects_orphans() -> None:
    report = cross_dataset_validation_flow()
    assert report.total_orphans > 0
    assert report.passed is False
