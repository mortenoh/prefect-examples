"""Tests for flow 071 -- Cross-Source Enrichment."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "data_engineering_cross_source_enrichment",
    Path(__file__).resolve().parent.parent.parent
    / "flows"
    / "data_engineering"
    / "data_engineering_cross_source_enrichment.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["data_engineering_cross_source_enrichment"] = _mod
_spec.loader.exec_module(_mod)

BaseRecord = _mod.BaseRecord
EnrichedRecord = _mod.EnrichedRecord
EnrichmentReport = _mod.EnrichmentReport
fetch_base_records = _mod.fetch_base_records
enrich_from_demographics = _mod.enrich_from_demographics
enrich_from_financials = _mod.enrich_from_financials
enrich_from_geography = _mod.enrich_from_geography
merge_enrichments = _mod.merge_enrichments
enrichment_summary = _mod.enrichment_summary
cross_source_enrichment_flow = _mod.cross_source_enrichment_flow


def test_base_record_model() -> None:
    r = BaseRecord(id=1, name="Test", region="US")
    assert r.id == 1


def test_fetch_base_records() -> None:
    records = fetch_base_records.fn()
    assert len(records) == 10
    assert all(isinstance(r, BaseRecord) for r in records)


def test_demographics_available() -> None:
    result = enrich_from_demographics.fn(1)
    assert result is not None
    assert "age_group" in result


def test_demographics_unavailable() -> None:
    result = enrich_from_demographics.fn(4)
    assert result is None


def test_financials_unavailable() -> None:
    result = enrich_from_financials.fn(5)
    assert result is None


def test_merge_all_sources() -> None:
    base = BaseRecord(id=1, name="Test", region="US")
    result = merge_enrichments.fn(base, {"a": 1}, {"b": 2}, {"c": 3})
    assert result.enrichment_completeness == 1.0


def test_merge_partial() -> None:
    base = BaseRecord(id=1, name="Test", region="US")
    result = merge_enrichments.fn(base, {"a": 1}, None, {"c": 3})
    assert result.enrichment_completeness == round(2 / 3, 2)


def test_flow_runs() -> None:
    state = cross_source_enrichment_flow(return_state=True)
    assert state.is_completed()
