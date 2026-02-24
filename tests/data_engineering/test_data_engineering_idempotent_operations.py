"""Tests for flow 078 -- Idempotent Operations."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "data_engineering_idempotent_operations",
    Path(__file__).resolve().parent.parent.parent
    / "flows"
    / "data_engineering"
    / "data_engineering_idempotent_operations.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["data_engineering_idempotent_operations"] = _mod
_spec.loader.exec_module(_mod)

OperationRecord = _mod.OperationRecord
IdempotencyRegistry = _mod.IdempotencyRegistry
IdempotencyReport = _mod.IdempotencyReport
compute_operation_id = _mod.compute_operation_id
check_registry = _mod.check_registry
execute_operation = _mod.execute_operation
register_operation = _mod.register_operation
idempotent_execute = _mod.idempotent_execute
idempotency_report = _mod.idempotency_report
idempotent_operations_flow = _mod.idempotent_operations_flow


def test_operation_id_deterministic() -> None:
    id1 = compute_operation_id.fn("test", {"a": 1})
    id2 = compute_operation_id.fn("test", {"a": 1})
    assert id1 == id2


def test_operation_id_different() -> None:
    id1 = compute_operation_id.fn("test", {"a": 1})
    id2 = compute_operation_id.fn("test", {"a": 2})
    assert id1 != id2


def test_check_registry_miss() -> None:
    registry = IdempotencyRegistry()
    assert check_registry.fn(registry, "nonexistent") is None


def test_execute_transform() -> None:
    result = execute_operation.fn("transform", {"value": 5})
    assert result["output"] == 10


def test_idempotent_execute_first_time() -> None:
    registry = IdempotencyRegistry()
    registry, result, skipped = idempotent_execute.fn(registry, "transform", {"value": 5})
    assert skipped is False
    assert result["output"] == 10


def test_idempotent_execute_duplicate() -> None:
    registry = IdempotencyRegistry()
    registry, _, _ = idempotent_execute.fn(registry, "transform", {"value": 5})
    registry, result, skipped = idempotent_execute.fn(registry, "transform", {"value": 5})
    assert skipped is True
    assert result["output"] == 10


def test_flow_runs() -> None:
    state = idempotent_operations_flow(return_state=True)
    assert state.is_completed()


def test_flow_skips_duplicates() -> None:
    report = idempotent_operations_flow()
    assert report.executed == 4
    assert report.skipped == 2
