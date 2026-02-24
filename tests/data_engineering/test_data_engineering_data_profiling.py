"""Tests for flow 067 -- Data Profiling."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "data_engineering_data_profiling",
    Path(__file__).resolve().parent.parent.parent / "flows" / "data_engineering" / "data_engineering_data_profiling.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["data_engineering_data_profiling"] = _mod
_spec.loader.exec_module(_mod)

ColumnProfile = _mod.ColumnProfile
DatasetProfile = _mod.DatasetProfile
generate_dataset = _mod.generate_dataset
infer_column_type = _mod.infer_column_type
profile_numeric_column = _mod.profile_numeric_column
profile_string_column = _mod.profile_string_column
profile_column = _mod.profile_column
profile_dataset = _mod.profile_dataset
data_profiling_flow = _mod.data_profiling_flow


def test_column_profile_model() -> None:
    p = ColumnProfile(name="x", dtype="numeric", count=10, null_count=0, null_rate=0.0)
    assert p.name == "x"


def test_infer_numeric() -> None:
    assert infer_column_type.fn([1, 2, 3.0]) == "numeric"


def test_infer_string() -> None:
    assert infer_column_type.fn(["a", "b"]) == "string"


def test_infer_empty() -> None:
    assert infer_column_type.fn([None, None]) == "empty"


def test_profile_numeric_column() -> None:
    profile = profile_numeric_column.fn("val", [10.0, 20.0, 30.0, None])
    assert profile.dtype == "numeric"
    assert profile.null_count == 1
    assert profile.mean is not None
    assert profile.mean == 20.0


def test_profile_string_column() -> None:
    profile = profile_string_column.fn("cat", ["alpha", "beta", None])
    assert profile.dtype == "string"
    assert profile.null_count == 1
    assert profile.unique_count == 2


def test_profile_dataset() -> None:
    data = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    profile = profile_dataset.fn("test", data)
    assert isinstance(profile, DatasetProfile)
    assert profile.row_count == 2
    assert profile.column_count == 2


def test_profile_dataset_empty() -> None:
    profile = profile_dataset.fn("empty", [])
    assert profile.row_count == 0
    assert profile.completeness_score == 0.0


def test_flow_runs() -> None:
    state = data_profiling_flow(return_state=True)
    assert state.is_completed()
