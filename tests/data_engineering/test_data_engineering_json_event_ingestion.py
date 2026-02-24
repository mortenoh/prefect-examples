"""Tests for flow 062 -- JSON Event Ingestion."""

import importlib.util
import json
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "data_engineering_json_event_ingestion",
    Path(__file__).resolve().parent.parent.parent
    / "flows"
    / "data_engineering"
    / "data_engineering_json_event_ingestion.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["data_engineering_json_event_ingestion"] = _mod
_spec.loader.exec_module(_mod)

FlattenedEvent = _mod.FlattenedEvent
IngestionResult = _mod.IngestionResult
generate_events = _mod.generate_events
load_json_events = _mod.load_json_events
flatten_dict = _mod.flatten_dict
normalize_event = _mod.normalize_event
write_ndjson = _mod.write_ndjson
compute_ingestion_stats = _mod.compute_ingestion_stats
json_event_ingestion_flow = _mod.json_event_ingestion_flow


def test_flattened_event_model() -> None:
    event = FlattenedEvent(event_id="e1", timestamp="2025-01-01T00:00:00Z", flat_fields={"a": 1})
    assert event.event_id == "e1"


def test_generate_events(tmp_path: Path) -> None:
    path = generate_events.fn(tmp_path / "events.json", count=3)
    data = json.loads(path.read_text())
    assert len(data) == 3


def test_flatten_dict_simple() -> None:
    result = flatten_dict.fn({"a": 1, "b": {"c": 2, "d": {"e": 3}}})
    assert result == {"a": 1, "b.c": 2, "b.d.e": 3}


def test_flatten_dict_with_list() -> None:
    result = flatten_dict.fn({"tags": ["a", "b"]})
    assert result == {"tags.0": "a", "tags.1": "b"}


def test_normalize_event() -> None:
    event = {"event_id": "e1", "timestamp": "2025-01-01T00:00:00Z", "data": {"x": 1}}
    result = normalize_event.fn(event)
    assert isinstance(result, FlattenedEvent)
    assert "data.x" in result.flat_fields


def test_write_ndjson(tmp_path: Path) -> None:
    events = [
        FlattenedEvent(event_id="e1", timestamp="t1", flat_fields={"a": 1}),
        FlattenedEvent(event_id="e2", timestamp="t2", flat_fields={"b": 2}),
    ]
    path = write_ndjson.fn(events, tmp_path / "out.ndjson")
    lines = path.read_text().strip().split("\n")
    assert len(lines) == 2
    parsed = json.loads(lines[0])
    assert parsed["event_id"] == "e1"


def test_flow_runs(tmp_path: Path) -> None:
    state = json_event_ingestion_flow(work_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
