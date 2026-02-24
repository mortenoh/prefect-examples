"""Tests for flow 030 — Table and Link Artifacts."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "core_table_and_link_artifacts",
    Path(__file__).resolve().parent.parent.parent / "flows" / "core" / "core_table_and_link_artifacts.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["core_table_and_link_artifacts"] = _mod
_spec.loader.exec_module(_mod)

compute_inventory = _mod.compute_inventory
publish_table = _mod.publish_table
publish_links = _mod.publish_links
table_and_link_artifacts_flow = _mod.table_and_link_artifacts_flow


def test_compute_inventory() -> None:
    result = compute_inventory.fn()
    assert isinstance(result, list)
    assert len(result) == 5
    assert "item" in result[0]
    assert "quantity" in result[0]
    assert "status" in result[0]


def test_publish_table() -> None:
    inventory = [{"item": "A", "quantity": 10, "status": "In Stock"}]
    # publish_table returns None — just verify no exception
    publish_table.fn(inventory)


def test_publish_links() -> None:
    # publish_links returns None — just verify no exception
    publish_links.fn()


def test_flow_runs() -> None:
    state = table_and_link_artifacts_flow(return_state=True)
    assert state.is_completed()
