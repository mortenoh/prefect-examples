"""Tests for flow 020 — Complex Pipeline."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "basics_complex_pipeline",
    Path(__file__).resolve().parent.parent.parent / "flows" / "basics" / "basics_complex_pipeline.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["basics_complex_pipeline"] = _mod
_spec.loader.exec_module(_mod)

validate_record = _mod.validate_record
enrich_record = _mod.enrich_record
extract_stage = _mod.extract_stage
load_stage = _mod.load_stage


def test_validate_record_returns_dict_with_valid_key() -> None:
    result = validate_record.fn({"id": 1})
    assert isinstance(result, dict)
    assert "valid" in result


def test_enrich_record_returns_dict_with_enriched_key() -> None:
    result = enrich_record.fn({"id": 1})
    assert isinstance(result, dict)
    assert "enriched" in result


def test_extract_stage() -> None:
    result = extract_stage()
    assert isinstance(result, list)
    assert len(result) > 0


def test_load_stage() -> None:
    data = [{"id": 1, "valid": True, "enriched": True}]
    result = load_stage(data)
    assert isinstance(result, str)
    assert "Loaded" in result
