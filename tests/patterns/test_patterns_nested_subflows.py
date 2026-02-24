"""Tests for flow 054 -- Nested Subflows."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "patterns_nested_subflows",
    Path(__file__).resolve().parent.parent.parent / "flows" / "patterns" / "patterns_nested_subflows.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["patterns_nested_subflows"] = _mod
_spec.loader.exec_module(_mod)

fetch_source_a = _mod.fetch_source_a
fetch_source_b = _mod.fetch_source_b
extract_group = _mod.extract_group
clean_record = _mod.clean_record
enrich_record = _mod.enrich_record
transform_group = _mod.transform_group
write_to_target = _mod.write_to_target
load_group = _mod.load_group
nested_subflows_flow = _mod.nested_subflows_flow


def test_fetch_source_a() -> None:
    result = fetch_source_a.fn()
    assert isinstance(result, list)
    assert result[0]["source"] == "A"


def test_fetch_source_b() -> None:
    result = fetch_source_b.fn()
    assert isinstance(result, list)
    assert result[0]["source"] == "B"


def test_extract_group() -> None:
    result = extract_group()
    assert isinstance(result, list)
    assert len(result) == 2


def test_clean_record() -> None:
    result = clean_record.fn({"id": 1, "source": "A", "value": 10})
    assert result["cleaned"] is True


def test_enrich_record() -> None:
    result = enrich_record.fn({"id": 1, "value": 10, "cleaned": True})
    assert result["enriched"] is True
    assert result["doubled_value"] == 20


def test_transform_group() -> None:
    records = [{"id": 1, "source": "A", "value": 10}]
    result = transform_group(records)
    assert len(result) == 1
    assert result[0]["enriched"] is True


def test_write_to_target() -> None:
    result = write_to_target.fn([{"id": 1}, {"id": 2}])
    assert "2 records" in result


def test_pipeline_end_to_end() -> None:
    """Test the full pipeline by calling subflows in sequence."""
    raw = extract_group()
    transformed = transform_group(raw)
    result = load_group(transformed)
    assert "Verified" in result
    assert "2 records" in result
