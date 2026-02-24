"""062 -- JSON Event Ingestion.

Recursive nested JSON flattening with NDJSON (newline-delimited JSON) output.
Handles deeply nested event payloads by flattening into dot-separated keys.

Airflow equivalent: JSON event stream to Parquet (DAG 064).
Prefect approach:    @task functions for each transformation step; @flow
                     orchestrates the pipeline.
"""

import json
import tempfile
from pathlib import Path

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class FlattenedEvent(BaseModel):
    """A flattened event with dot-separated keys."""

    event_id: str
    timestamp: str
    flat_fields: dict


class IngestionResult(BaseModel):
    """Summary of the ingestion pipeline."""

    total_events: int
    flattened_events: int
    max_depth: int
    output_path: str


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def generate_events(path: Path, count: int = 5) -> Path:
    """Generate nested JSON events and write to file.

    Args:
        path: Output file path.
        count: Number of events to generate.

    Returns:
        Path to the generated file.
    """
    events = []
    for i in range(1, count + 1):
        events.append(
            {
                "event_id": f"evt_{i:04d}",
                "timestamp": f"2025-01-{i:02d}T10:00:00Z",
                "user": {
                    "id": f"u{i}",
                    "profile": {
                        "name": f"User {i}",
                        "settings": {
                            "theme": "dark" if i % 2 == 0 else "light",
                            "notifications": True,
                        },
                    },
                },
                "payload": {
                    "action": "click" if i % 3 != 0 else "view",
                    "metadata": {
                        "source": "web",
                        "version": f"1.{i}",
                    },
                },
                "tags": [f"tag_{i}", f"tag_{i + 10}"],
            }
        )
    path.write_text(json.dumps(events, indent=2))
    print(f"Generated {count} events to {path}")
    return path


@task
def load_json_events(path: Path) -> list[dict]:
    """Load JSON events from a file.

    Args:
        path: Path to the JSON file.

    Returns:
        List of event dicts.
    """
    data = json.loads(path.read_text())
    print(f"Loaded {len(data)} events from {path.name}")
    return data


@task
def flatten_dict(data: dict, prefix: str = "", separator: str = ".") -> dict:
    """Recursively flatten a nested dict into dot-separated keys.

    Args:
        data: The nested dict to flatten.
        prefix: Key prefix for recursion.
        separator: Separator between nested keys.

    Returns:
        A flat dict with dot-separated keys.
    """
    items: dict = {}
    for key, value in data.items():
        new_key = f"{prefix}{separator}{key}" if prefix else key
        if isinstance(value, dict):
            items.update(flatten_dict.fn(value, new_key, separator))
        elif isinstance(value, list):
            for idx, item in enumerate(value):
                list_key = f"{new_key}{separator}{idx}"
                if isinstance(item, dict):
                    items.update(flatten_dict.fn(item, list_key, separator))
                else:
                    items[list_key] = item
        else:
            items[new_key] = value
    return items


@task
def normalize_event(event: dict) -> FlattenedEvent:
    """Flatten a single event into a FlattenedEvent.

    Args:
        event: A raw event dict.

    Returns:
        A FlattenedEvent with flat_fields.
    """
    flat = flatten_dict.fn(event)
    return FlattenedEvent(
        event_id=event["event_id"],
        timestamp=event["timestamp"],
        flat_fields=flat,
    )


@task
def write_ndjson(events: list[FlattenedEvent], output_path: Path) -> Path:
    """Write flattened events as NDJSON.

    Args:
        events: List of FlattenedEvent objects.
        output_path: Path to write NDJSON file.

    Returns:
        Path to the written file.
    """
    with open(output_path, "w") as f:
        for event in events:
            line = json.dumps(event.model_dump())
            f.write(line + "\n")
    print(f"Wrote {len(events)} events to {output_path}")
    return output_path


@task
def compute_ingestion_stats(events: list[FlattenedEvent], output_path: str) -> IngestionResult:
    """Compute statistics about the ingestion.

    Args:
        events: The flattened events.
        output_path: Path where output was written.

    Returns:
        Ingestion result summary.
    """

    def _depth(d: dict, level: int = 0) -> int:
        if not isinstance(d, dict) or not d:
            return level
        return max(_depth(v, level + 1) for v in d.values())

    max_depth = max(_depth(e.flat_fields) for e in events) if events else 0
    return IngestionResult(
        total_events=len(events),
        flattened_events=len(events),
        max_depth=max_depth,
        output_path=output_path,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="062_json_event_ingestion", log_prints=True)
def json_event_ingestion_flow(work_dir: str | None = None, event_count: int = 5) -> IngestionResult:
    """Ingest nested JSON events, flatten, and write as NDJSON.

    Args:
        work_dir: Working directory. Uses temp dir if not provided.
        event_count: Number of events to generate.

    Returns:
        Ingestion result summary.
    """
    if work_dir is None:
        work_dir = tempfile.mkdtemp(prefix="json_ingestion_")

    base = Path(work_dir)
    base.mkdir(parents=True, exist_ok=True)

    input_path = base / "events.json"
    output_path = base / "events.ndjson"

    # Generate and load
    generate_events(input_path, count=event_count)
    raw_events = load_json_events(input_path)

    # Flatten each event
    flattened = [normalize_event(event) for event in raw_events]

    # Write NDJSON
    write_ndjson(flattened, output_path)

    # Compute stats
    result = compute_ingestion_stats(flattened, str(output_path))
    print(f"Ingestion complete: {result.total_events} events processed")
    return result


if __name__ == "__main__":
    json_event_ingestion_flow()
