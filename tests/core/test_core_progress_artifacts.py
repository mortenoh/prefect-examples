"""Tests for Progress Artifacts flow."""

import importlib.util
import sys
from pathlib import Path

# Digit-prefixed filenames can't be imported normally -- use importlib.
_spec = importlib.util.spec_from_file_location(
    "core_progress_artifacts",
    Path(__file__).resolve().parent.parent.parent / "flows" / "core" / "core_progress_artifacts.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["core_progress_artifacts"] = _mod
_spec.loader.exec_module(_mod)

process_batch = _mod.process_batch
publish_summary = _mod.publish_summary
progress_artifacts_flow = _mod.progress_artifacts_flow


def test_process_batch() -> None:
    result = process_batch.fn(["x1", "x2", "x3"], "test")
    assert isinstance(result, dict)
    assert result["label"] == "test"
    assert result["item_count"] == 3
    assert "duration" in result


def test_publish_summary() -> None:
    batch_results = [
        {"label": "alpha", "item_count": 4, "duration": 0.2},
        {"label": "beta", "item_count": 3, "duration": 0.15},
    ]
    result = publish_summary.fn(batch_results)
    assert isinstance(result, str)
    assert "alpha" in result
    assert "beta" in result
    assert "Batch Processing Summary" in result


def test_flow_runs() -> None:
    state = progress_artifacts_flow(return_state=True)
    assert state.is_completed()
