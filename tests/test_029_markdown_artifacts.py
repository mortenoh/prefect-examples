"""Tests for flow 029 — Markdown Artifacts."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally — use importlib.
_spec = importlib.util.spec_from_file_location(
    "flow_029",
    Path(__file__).resolve().parent.parent / "flows" / "029_markdown_artifacts.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_029"] = _mod
_spec.loader.exec_module(_mod)

generate_data = _mod.generate_data
publish_report = _mod.publish_report
markdown_artifacts_flow = _mod.markdown_artifacts_flow


def test_generate_data() -> None:
    result = generate_data.fn()
    assert isinstance(result, list)
    assert len(result) == 5
    assert "name" in result[0]
    assert "score" in result[0]


def test_publish_report() -> None:
    data = [{"name": "Alice", "department": "Eng", "score": 90}]
    result = publish_report.fn(data)
    assert isinstance(result, str)
    assert "Alice" in result
    assert "Performance Report" in result


def test_flow_runs() -> None:
    state = markdown_artifacts_flow(return_state=True)
    assert state.is_completed()
