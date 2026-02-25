"""Streaming Batch Processor.

Windowed batch processing with anomaly detection and trend analysis.
Splits a data stream into fixed-size windows, computes per-window statistics,
detects anomalies (values > 3 stdev from mean), and identifies trends.

Airflow equivalent: GeoJSON parsing, OData pivoting (DAGs 091, 095).
Prefect approach:    Windowed processing with .map(), statistics module
                     for per-window aggregation.
"""

import random
import statistics
from typing import Any

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class BatchWindow(BaseModel):
    """Definition of a processing window."""

    window_id: int
    start_index: int
    end_index: int
    record_count: int


class WindowResult(BaseModel):
    """Result of processing a single window."""

    window_id: int
    record_count: int
    mean: float
    stdev: float
    min_val: float
    max_val: float
    median: float
    anomaly_count: int
    anomalies: list[dict[str, Any]]


class StreamResult(BaseModel):
    """Global result of processing the entire stream."""

    total_records: int
    total_windows: int
    global_mean: float
    global_stdev: float
    total_anomalies: int
    window_results: list[WindowResult]
    trends: list[dict[str, Any]]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def generate_stream(total_records: int = 100, seed: int = 42) -> list[dict[str, Any]]:
    """Generate a synthetic data stream with some anomalies.

    Args:
        total_records: Number of records to generate.
        seed: Random seed for reproducibility.

    Returns:
        List of record dicts with index and value.
    """
    rng = random.Random(seed)
    data = []
    for i in range(total_records):
        # Normal values with occasional spikes
        base = 50.0 + rng.gauss(0, 10)
        if i % 25 == 0 and i > 0:
            base += rng.choice([-1, 1]) * 60  # anomaly spike
        data.append({"index": i, "value": round(base, 2), "category": ["A", "B", "C"][i % 3]})
    print(f"Generated stream with {total_records} records")
    return data


@task
def create_windows(data: list[dict[str, Any]], window_size: int = 20) -> list[BatchWindow]:
    """Split data into fixed-size windows.

    Args:
        data: Full data stream.
        window_size: Records per window.

    Returns:
        List of BatchWindow definitions.
    """
    windows: list[BatchWindow] = []
    for i in range(0, len(data), window_size):
        end = min(i + window_size, len(data))
        windows.append(
            BatchWindow(
                window_id=len(windows),
                start_index=i,
                end_index=end,
                record_count=end - i,
            )
        )
    print(f"Created {len(windows)} windows of size {window_size}")
    return windows


@task
def process_window(data: list[dict[str, Any]], window: BatchWindow) -> WindowResult:
    """Process a single window: compute stats and detect anomalies.

    Args:
        data: Full data stream.
        window: Window definition.

    Returns:
        WindowResult with statistics and anomalies.
    """
    records = data[window.start_index : window.end_index]
    values = [r["value"] for r in records]

    mean = statistics.mean(values)
    stdev = statistics.stdev(values) if len(values) >= 2 else 0.0
    med = statistics.median(values)

    # Anomaly detection: > 3 stdev from mean
    threshold = 3 * stdev if stdev > 0 else float("inf")
    anomalies = []
    for r in records:
        if abs(r["value"] - mean) > threshold:
            anomalies.append(
                {
                    "index": r["index"],
                    "value": r["value"],
                    "deviation": round(abs(r["value"] - mean) / stdev if stdev > 0 else 0, 2),
                }
            )

    return WindowResult(
        window_id=window.window_id,
        record_count=len(records),
        mean=round(mean, 2),
        stdev=round(stdev, 2),
        min_val=min(values),
        max_val=max(values),
        median=round(med, 2),
        anomaly_count=len(anomalies),
        anomalies=anomalies,
    )


@task
def merge_window_results(results: list[WindowResult]) -> StreamResult:
    """Merge per-window results into global statistics.

    Args:
        results: List of window results.

    Returns:
        StreamResult without trends (added separately).
    """
    all_means = [r.mean for r in results]
    total_records = sum(r.record_count for r in results)
    total_anomalies = sum(r.anomaly_count for r in results)
    global_mean = round(statistics.mean(all_means), 2) if all_means else 0.0
    global_stdev = round(statistics.stdev(all_means), 2) if len(all_means) >= 2 else 0.0

    return StreamResult(
        total_records=total_records,
        total_windows=len(results),
        global_mean=global_mean,
        global_stdev=global_stdev,
        total_anomalies=total_anomalies,
        window_results=results,
        trends=[],
    )


@task
def detect_trends(results: list[WindowResult]) -> list[dict[str, Any]]:
    """Detect trends between consecutive windows.

    Args:
        results: Ordered list of window results.

    Returns:
        List of trend dicts (rising/falling/stable between consecutive windows).
    """
    trends = []
    for i in range(1, len(results)):
        prev = results[i - 1]
        curr = results[i]
        diff = curr.mean - prev.mean
        if diff > 2.0:
            direction = "rising"
        elif diff < -2.0:
            direction = "falling"
        else:
            direction = "stable"
        trends.append(
            {
                "from_window": prev.window_id,
                "to_window": curr.window_id,
                "mean_change": round(diff, 2),
                "direction": direction,
            }
        )
    return trends


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="data_engineering_streaming_batch", log_prints=True)
def streaming_batch_flow(
    total_records: int = 100,
    window_size: int = 20,
    seed: int = 42,
) -> StreamResult:
    """Process a data stream in windowed batches with anomaly detection.

    Args:
        total_records: Number of records in the stream.
        window_size: Records per processing window.
        seed: Random seed for reproducibility.

    Returns:
        StreamResult with global statistics, anomalies, and trends.
    """
    data = generate_stream(total_records, seed)
    windows = create_windows(data, window_size)

    # Process windows in parallel
    futures = process_window.map([data] * len(windows), windows)
    window_results = [f.result() for f in futures]

    stream_result = merge_window_results(window_results)
    trends = detect_trends(window_results)
    stream_result = stream_result.model_copy(update={"trends": trends})

    print(
        f"Stream processing complete: {stream_result.total_records} records, "
        f"{stream_result.total_anomalies} anomalies, {len(trends)} trends"
    )
    return stream_result


if __name__ == "__main__":
    streaming_batch_flow()
