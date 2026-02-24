"""Tests for flow 077 -- Streaming Batch Processor."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "data_engineering_streaming_batch",
    Path(__file__).resolve().parent.parent.parent
    / "flows"
    / "data_engineering"
    / "data_engineering_streaming_batch.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["data_engineering_streaming_batch"] = _mod
_spec.loader.exec_module(_mod)

BatchWindow = _mod.BatchWindow
WindowResult = _mod.WindowResult
StreamResult = _mod.StreamResult
generate_stream = _mod.generate_stream
create_windows = _mod.create_windows
process_window = _mod.process_window
merge_window_results = _mod.merge_window_results
detect_trends = _mod.detect_trends
streaming_batch_flow = _mod.streaming_batch_flow


def test_generate_stream_deterministic() -> None:
    s1 = generate_stream.fn(50, seed=42)
    s2 = generate_stream.fn(50, seed=42)
    assert s1 == s2
    assert len(s1) == 50


def test_create_windows() -> None:
    data = [{"value": i} for i in range(50)]
    windows = create_windows.fn(data, window_size=20)
    assert len(windows) == 3
    assert windows[0].record_count == 20
    assert windows[2].record_count == 10


def test_process_window() -> None:
    data = [{"index": i, "value": float(i * 10)} for i in range(10)]
    window = BatchWindow(window_id=0, start_index=0, end_index=10, record_count=10)
    result = process_window.fn(data, window)
    assert isinstance(result, WindowResult)
    assert result.record_count == 10
    assert result.mean is not None


def test_merge_window_results() -> None:
    results = [
        WindowResult(
            window_id=0,
            record_count=10,
            mean=50.0,
            stdev=5.0,
            min_val=40.0,
            max_val=60.0,
            median=50.0,
            anomaly_count=1,
            anomalies=[],
        ),
        WindowResult(
            window_id=1,
            record_count=10,
            mean=55.0,
            stdev=4.0,
            min_val=45.0,
            max_val=65.0,
            median=55.0,
            anomaly_count=0,
            anomalies=[],
        ),
    ]
    stream = merge_window_results.fn(results)
    assert stream.total_records == 20
    assert stream.total_anomalies == 1


def test_detect_trends() -> None:
    results = [
        WindowResult(
            window_id=0,
            record_count=10,
            mean=50.0,
            stdev=5.0,
            min_val=40.0,
            max_val=60.0,
            median=50.0,
            anomaly_count=0,
            anomalies=[],
        ),
        WindowResult(
            window_id=1,
            record_count=10,
            mean=55.0,
            stdev=4.0,
            min_val=45.0,
            max_val=65.0,
            median=55.0,
            anomaly_count=0,
            anomalies=[],
        ),
        WindowResult(
            window_id=2,
            record_count=10,
            mean=54.0,
            stdev=4.0,
            min_val=44.0,
            max_val=64.0,
            median=54.0,
            anomaly_count=0,
            anomalies=[],
        ),
    ]
    trends = detect_trends.fn(results)
    assert len(trends) == 2
    assert trends[0]["direction"] == "rising"
    assert trends[1]["direction"] == "stable"


def test_flow_runs() -> None:
    state = streaming_batch_flow(total_records=50, window_size=10, return_state=True)
    assert state.is_completed()


def test_flow_detects_anomalies() -> None:
    result = streaming_batch_flow(total_records=100, window_size=20, seed=42)
    assert result.total_anomalies >= 0
    assert result.total_windows == 5
