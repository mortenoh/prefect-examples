"""Tests for flow 070 -- API Pagination."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_070",
    Path(__file__).resolve().parent.parent / "flows" / "070_api_pagination.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_070"] = _mod
_spec.loader.exec_module(_mod)

PageResponse = _mod.PageResponse
PaginationResult = _mod.PaginationResult
simulate_api_page = _mod.simulate_api_page
fetch_all_pages = _mod.fetch_all_pages
chunk_records = _mod.chunk_records
process_chunk = _mod.process_chunk
combine_chunk_results = _mod.combine_chunk_results
api_pagination_flow = _mod.api_pagination_flow


def test_page_response_model() -> None:
    p = PageResponse(page=1, records=[{"id": 1}], total_pages=3, has_next=True)
    assert p.has_next is True


def test_simulate_api_page_first() -> None:
    page = simulate_api_page.fn(1, page_size=10, total_records=25)
    assert page.page == 1
    assert len(page.records) == 10
    assert page.has_next is True


def test_simulate_api_page_last() -> None:
    page = simulate_api_page.fn(3, page_size=10, total_records=25)
    assert len(page.records) == 5
    assert page.has_next is False


def test_fetch_all_pages() -> None:
    pages = fetch_all_pages.fn(page_size=10, total_records=25)
    assert len(pages) == 3
    total = sum(len(p.records) for p in pages)
    assert total == 25


def test_chunk_records() -> None:
    records = [{"id": i} for i in range(10)]
    chunks = chunk_records.fn(records, chunk_size=3)
    assert len(chunks) == 4
    assert len(chunks[-1]) == 1


def test_process_chunk() -> None:
    records = [{"id": 1, "value": 10.0, "category": "A"}, {"id": 2, "value": 20.0, "category": "B"}]
    result = process_chunk.fn(records, 0)
    assert result["record_count"] == 2
    assert result["total_value"] == 30.0


def test_flow_runs() -> None:
    state = api_pagination_flow(return_state=True)
    assert state.is_completed()
