"""Tests for flow 074 -- Producer-Consumer."""

import importlib.util
import json
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "data_engineering_producer_consumer",
    Path(__file__).resolve().parent.parent.parent
    / "flows"
    / "data_engineering"
    / "data_engineering_producer_consumer.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["data_engineering_producer_consumer"] = _mod
_spec.loader.exec_module(_mod)

DataPackage = _mod.DataPackage
ConsumerResult = _mod.ConsumerResult
produce_data = _mod.produce_data
discover_packages = _mod.discover_packages
consume_package = _mod.consume_package
consumption_report = _mod.consumption_report
producer_flow = _mod.producer_flow
consumer_flow = _mod.consumer_flow
producer_consumer_flow = _mod.producer_consumer_flow


def test_data_package_model() -> None:
    p = DataPackage(
        producer_id="test",
        produced_at="2025-01-01T00:00:00+00:00",
        record_count=5,
        data_path="/tmp/data.json",
        metadata_path="/tmp/meta.json",
    )
    assert p.record_count == 5


def test_produce_data(tmp_path: Path) -> None:
    pkg = produce_data.fn(str(tmp_path), "test_producer", records=5)
    assert pkg.record_count == 5
    assert Path(pkg.data_path).exists()
    data = json.loads(Path(pkg.data_path).read_text())
    assert len(data) == 5


def test_discover_packages(tmp_path: Path) -> None:
    produce_data.fn(str(tmp_path), "p1", records=3)
    produce_data.fn(str(tmp_path), "p2", records=4)
    packages = discover_packages.fn(str(tmp_path))
    assert len(packages) == 2


def test_consume_package(tmp_path: Path) -> None:
    pkg = produce_data.fn(str(tmp_path), "test", records=5)
    result = consume_package.fn(pkg, "consumer_1")
    assert result.records_processed == 5
    assert result.consumer_id == "consumer_1"


def test_consumption_report() -> None:
    results = [
        ConsumerResult(consumer_id="c1", package_producer="p1", records_processed=5, latency_seconds=0.1),
        ConsumerResult(consumer_id="c1", package_producer="p2", records_processed=3, latency_seconds=0.2),
    ]
    report = consumption_report.fn(results)
    assert "8 total records" in report


def test_producer_flow(tmp_path: Path) -> None:
    pkg = producer_flow(str(tmp_path), "test")
    assert isinstance(pkg, DataPackage)


def test_consumer_flow(tmp_path: Path) -> None:
    produce_data.fn(str(tmp_path), "p1", records=5)
    results = consumer_flow(str(tmp_path), "c1")
    assert len(results) == 1
    assert results[0].records_processed == 5


def test_orchestrator_flow(tmp_path: Path) -> None:
    state = producer_consumer_flow(work_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
