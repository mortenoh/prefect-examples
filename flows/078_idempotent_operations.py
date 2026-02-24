"""078 -- Idempotent Operations.

Hash-based idempotency registry. Operations check the registry before
executing, making it safe to re-run the same operation any number of times.

Airflow equivalent: None (production resilience pattern).
Prefect approach:    Hash-based operation IDs, registry dict, skip-if-exists.
"""

import datetime
import hashlib

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class OperationRecord(BaseModel):
    """Record of a completed operation."""

    operation_id: str
    name: str
    input_hash: str
    result: dict
    completed_at: str


class IdempotencyRegistry(BaseModel):
    """Registry of completed operations."""

    operations: dict[str, OperationRecord] = {}


class IdempotencyReport(BaseModel):
    """Summary of idempotent execution."""

    total_operations: int
    executed: int
    skipped: int
    operations_log: list[dict]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def compute_operation_id(name: str, inputs: dict) -> str:
    """Compute a unique operation ID from name and inputs.

    Args:
        name: Operation name.
        inputs: Operation inputs.

    Returns:
        Hex digest string.
    """
    raw = f"{name}:{sorted(inputs.items())}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


@task
def check_registry(registry: IdempotencyRegistry, operation_id: str) -> OperationRecord | None:
    """Check if an operation has already been completed.

    Args:
        registry: The idempotency registry.
        operation_id: Operation ID to check.

    Returns:
        OperationRecord if found, None otherwise.
    """
    return registry.operations.get(operation_id)


@task
def execute_operation(name: str, inputs: dict) -> dict:
    """Execute an operation (simulated).

    Args:
        name: Operation name.
        inputs: Operation inputs.

    Returns:
        Operation result dict.
    """
    if name == "transform":
        value = inputs.get("value", 0)
        return {"output": value * 2, "operation": name}
    elif name == "aggregate":
        values = inputs.get("values", [])
        return {"output": sum(values), "operation": name}
    elif name == "validate":
        data = inputs.get("data", "")
        return {"output": len(data) > 0, "operation": name}
    return {"output": None, "operation": name}


@task
def register_operation(
    registry: IdempotencyRegistry,
    operation_id: str,
    name: str,
    input_hash: str,
    result: dict,
) -> IdempotencyRegistry:
    """Register a completed operation.

    Args:
        registry: Current registry.
        operation_id: Operation ID.
        name: Operation name.
        input_hash: Hash of inputs.
        result: Operation result.

    Returns:
        Updated registry.
    """
    record = OperationRecord(
        operation_id=operation_id,
        name=name,
        input_hash=input_hash,
        result=result,
        completed_at=datetime.datetime.now(datetime.UTC).isoformat(),
    )
    registry.operations[operation_id] = record
    return registry


@task
def idempotent_execute(
    registry: IdempotencyRegistry,
    name: str,
    inputs: dict,
) -> tuple[IdempotencyRegistry, dict, bool]:
    """Execute an operation idempotently.

    Args:
        registry: Current registry.
        name: Operation name.
        inputs: Operation inputs.

    Returns:
        Tuple of (updated registry, result, was_skipped).
    """
    op_id = compute_operation_id.fn(name, inputs)
    existing = check_registry.fn(registry, op_id)

    if existing is not None:
        print(f"SKIP: '{name}' already executed (id={op_id})")
        return registry, existing.result, True

    result = execute_operation.fn(name, inputs)
    registry = register_operation.fn(registry, op_id, name, op_id, result)
    print(f"EXEC: '{name}' completed (id={op_id})")
    return registry, result, False


@task
def idempotency_report(operations_log: list[dict]) -> IdempotencyReport:
    """Generate an idempotency report.

    Args:
        operations_log: Log of operations with skip status.

    Returns:
        IdempotencyReport.
    """
    executed = sum(1 for op in operations_log if not op["skipped"])
    skipped = sum(1 for op in operations_log if op["skipped"])
    return IdempotencyReport(
        total_operations=len(operations_log),
        executed=executed,
        skipped=skipped,
        operations_log=operations_log,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="078_idempotent_operations", log_prints=True)
def idempotent_operations_flow() -> IdempotencyReport:
    """Demonstrate idempotent operations with a registry.

    Returns:
        IdempotencyReport showing executed vs skipped operations.
    """
    registry = IdempotencyRegistry()
    log: list[dict] = []

    operations = [
        ("transform", {"value": 42}),
        ("aggregate", {"values": [1, 2, 3]}),
        ("validate", {"data": "hello"}),
        ("transform", {"value": 42}),  # duplicate
        ("aggregate", {"values": [1, 2, 3]}),  # duplicate
        ("transform", {"value": 99}),  # different inputs
    ]

    for name, inputs in operations:
        registry, result, skipped = idempotent_execute(registry, name, inputs)
        log.append({"name": name, "result": result, "skipped": skipped})

    report = idempotency_report(log)
    print(f"Idempotency: {report.executed} executed, {report.skipped} skipped")
    return report


if __name__ == "__main__":
    idempotent_operations_flow()
