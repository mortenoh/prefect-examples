"""Tests for flow 092 -- Data Transfer."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "analytics_data_transfer",
    Path(__file__).resolve().parent.parent.parent / "flows" / "analytics" / "analytics_data_transfer.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["analytics_data_transfer"] = _mod
_spec.loader.exec_module(_mod)

SourceRecord = _mod.SourceRecord
DestRecord = _mod.DestRecord
TransferVerification = _mod.TransferVerification
TransferResult = _mod.TransferResult
generate_source_data = _mod.generate_source_data
transform_for_dest = _mod.transform_for_dest
transfer_batch = _mod.transfer_batch
verify_transfer = _mod.verify_transfer
transfer_summary = _mod.transfer_summary
data_transfer_flow = _mod.data_transfer_flow


def test_generate_source_data() -> None:
    data = generate_source_data.fn()
    assert len(data) == 8
    assert all(isinstance(r, SourceRecord) for r in data)


def test_transform_for_dest_large() -> None:
    source = SourceRecord(id=1, city="Oslo", country="Norway", population=700000)
    dest = transform_for_dest.fn(source)
    assert dest.size_category == "large"
    assert dest.city == "Oslo"


def test_transform_for_dest_medium() -> None:
    source = SourceRecord(id=1, city="Bergen", country="Norway", population=285000)
    dest = transform_for_dest.fn(source)
    assert dest.size_category == "medium"


def test_transform_for_dest_small() -> None:
    source = SourceRecord(id=1, city="Reykjavik", country="Iceland", population=130000)
    dest = transform_for_dest.fn(source)
    assert dest.size_category == "small"


def test_transform_for_dest_town() -> None:
    source = SourceRecord(id=1, city="Tromso", country="Norway", population=77000)
    dest = transform_for_dest.fn(source)
    assert dest.size_category == "town"


def test_transfer_batch_count_match() -> None:
    sources = generate_source_data.fn()
    destinations = transfer_batch.fn(sources)
    assert len(destinations) == len(sources)


def test_verify_transfer() -> None:
    sources = generate_source_data.fn()
    destinations = transfer_batch.fn(sources)
    verification = verify_transfer.fn(sources, destinations)
    assert isinstance(verification, TransferVerification)
    assert verification.count_match is True
    assert verification.checksum_match is True


def test_flow_runs() -> None:
    state = data_transfer_flow(return_state=True)
    assert state.is_completed()
