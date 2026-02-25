"""Tests for Remote Result Storage flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch

_spec = importlib.util.spec_from_file_location(
    "core_remote_result_storage",
    Path(__file__).resolve().parent.parent.parent / "flows" / "core" / "core_remote_result_storage.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["core_remote_result_storage"] = _mod
_spec.loader.exec_module(_mod)

generate_report_data = _mod.generate_report_data
enrich_data = _mod.enrich_data
summarize = _mod.summarize
remote_result_storage_flow = _mod.remote_result_storage_flow


# ---------------------------------------------------------------------------
# Unit tests -- call .fn() directly (no orchestration overhead)
# ---------------------------------------------------------------------------


def test_generate_report_data() -> None:
    result = generate_report_data.fn("us-east")
    assert isinstance(result, dict)
    assert result["region"] == "us-east"
    assert result["total_sales"] == 150_000
    assert result["units_sold"] == 3_200
    assert "avg_price" in result
    assert "top_product" in result


def test_enrich_data_gold_tier() -> None:
    report = {
        "region": "us-east",
        "total_sales": 150_000,
        "units_sold": 3_200,
        "avg_price": 46.88,
        "top_product": "Widget Pro",
    }
    result = enrich_data.fn(report, "us-east")
    assert result["tier"] == "gold"
    assert result["margin_pct"] == 32.5
    assert result["growth_yoy"] == 12.1
    assert result["region"] == "us-east"


def test_enrich_data_silver_tier() -> None:
    report = {
        "region": "ap-south",
        "total_sales": 50_000,
        "units_sold": 1_000,
        "avg_price": 50.0,
        "top_product": "Widget Lite",
    }
    result = enrich_data.fn(report, "ap-south")
    assert result["tier"] == "silver"


def test_summarize() -> None:
    reports = [
        {"region": "us-east", "total_sales": 150_000, "units_sold": 3_200},
        {"region": "us-west", "total_sales": 120_000, "units_sold": 2_800},
    ]
    result = summarize.fn(reports)
    assert isinstance(result, str)
    assert "us-east" in result
    assert "us-west" in result
    assert "270,000" in result
    assert "6,000" in result


# ---------------------------------------------------------------------------
# Integration test -- run the full flow with S3 storage bypassed
# ---------------------------------------------------------------------------


def test_flow_runs() -> None:
    with patch("core_remote_result_storage.create_s3_storage", return_value=None):
        state = remote_result_storage_flow(return_state=True)
    assert state.is_completed()
