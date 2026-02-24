"""Tests for flow 040 — Production Pipeline."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "flow_040",
    Path(__file__).resolve().parent.parent / "flows" / "040_production_pipeline.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_040"] = _mod
_spec.loader.exec_module(_mod)

validate = _mod.validate
enrich = _mod.enrich
notify = _mod.notify
extract_stage = _mod.extract_stage
load_stage = _mod.load_stage


def test_enrich_adds_enriched_flag() -> None:
    result = enrich.fn({"id": 1, "valid": True})
    assert isinstance(result, dict)
    assert result["enriched"] is True
    assert result["id"] == 1


def test_enrich() -> None:
    result = enrich.fn({"id": 1, "valid": True})
    assert isinstance(result, dict)
    assert result["enriched"] is True
    assert "enriched_at" in result


def test_notify() -> None:
    # notify returns None — just verify no exception
    notify.fn("Test summary")


def test_extract_stage() -> None:
    result = extract_stage()
    assert isinstance(result, list)
    assert len(result) == 5
    assert "id" in result[0]


def test_load_stage() -> None:
    data = [
        {"id": 1, "valid": True, "enriched": True, "value": 100},
        {"id": 2, "valid": True, "enriched": False, "value": 200},
    ]
    result = load_stage(data)
    assert isinstance(result, str)
    assert "Loaded" in result
    assert "2" in result
