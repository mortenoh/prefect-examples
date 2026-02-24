"""Tests for flow 009 — Subflows."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "flow_009",
    Path(__file__).resolve().parent.parent / "flows" / "009_subflows.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_009"] = _mod
_spec.loader.exec_module(_mod)

extract_flow = _mod.extract_flow
transform_flow = _mod.transform_flow
load_flow = _mod.load_flow


def test_extract_flow() -> None:
    result = extract_flow()
    assert isinstance(result, list)
    assert len(result) > 0


def test_transform_flow() -> None:
    raw = [{"id": 1, "value": "alpha"}]
    result = transform_flow(raw)
    assert isinstance(result, list)
    assert result[0]["processed"] is True


def test_load_flow() -> None:
    data = [{"id": 1, "value": "alpha"}]
    result = load_flow(data)
    assert isinstance(result, str)
    assert "Loaded" in result


def test_pipeline_end_to_end() -> None:
    """Test the full pipeline by calling subflows in sequence."""
    raw = extract_flow()
    transformed = transform_flow(raw)
    summary = load_flow(transformed)
    assert "Loaded" in summary
    assert str(len(raw)) in summary
