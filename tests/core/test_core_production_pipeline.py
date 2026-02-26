"""Tests for flow 040 -- Production Pipeline."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally -- use importlib.
_spec = importlib.util.spec_from_file_location(
    "core_production_pipeline",
    Path(__file__).resolve().parent.parent.parent / "flows" / "core" / "core_production_pipeline.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["core_production_pipeline"] = _mod
_spec.loader.exec_module(_mod)

ProductionRecord = _mod.ProductionRecord
validate = _mod.validate
enrich = _mod.enrich
notify = _mod.notify
extract_stage = _mod.extract_stage
load_stage = _mod.load_stage


def test_enrich_adds_enriched_flag() -> None:
    record = ProductionRecord(id=1, name="Alice", value=100, valid=True)
    result = enrich.fn(record)
    assert isinstance(result, ProductionRecord)
    assert result.enriched is True
    assert result.id == 1


def test_enrich() -> None:
    record = ProductionRecord(id=1, name="Alice", value=100, valid=True)
    result = enrich.fn(record)
    assert isinstance(result, ProductionRecord)
    assert result.enriched is True
    assert result.enriched_at != ""


def test_notify() -> None:
    # notify returns None -- just verify no exception
    notify.fn("Test summary")


def test_extract_stage() -> None:
    result = extract_stage()
    assert isinstance(result, list)
    assert len(result) == 5
    assert isinstance(result[0], ProductionRecord)


def test_load_stage() -> None:
    data = [
        ProductionRecord(id=1, name="Alice", value=100, valid=True, enriched=True),
        ProductionRecord(id=2, name="Bob", value=200, valid=True),
    ]
    result = load_stage(data)
    assert isinstance(result, str)
    assert "Loaded" in result
    assert "2" in result
