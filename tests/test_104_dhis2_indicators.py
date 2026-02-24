"""Tests for flow 104 -- DHIS2 Indicators API."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_104",
    Path(__file__).resolve().parent.parent / "flows" / "104_dhis2_indicators.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_104"] = _mod
_spec.loader.exec_module(_mod)

Dhis2Connection = _mod.Dhis2Connection
RawIndicator = _mod.RawIndicator
FlatIndicator = _mod.FlatIndicator
IndicatorReport = _mod.IndicatorReport
fetch_indicators = _mod.fetch_indicators
flatten_indicators = _mod.flatten_indicators
write_indicator_csv = _mod.write_indicator_csv
indicator_report = _mod.indicator_report
dhis2_indicators_flow = _mod.dhis2_indicators_flow
_count_operands = _mod._count_operands
_count_operators = _mod._count_operators
_complexity_bin = _mod._complexity_bin


def test_count_operands_single() -> None:
    assert _count_operands("#{abc01.def01}") == 1


def test_count_operands_multiple() -> None:
    assert _count_operands("#{abc01.def01}+#{abc02.def02}-#{abc03.def03}") == 3


def test_count_operators() -> None:
    assert _count_operators("#{a}+#{b}-#{c}*#{d}/#{e}") == 4


def test_complexity_bins() -> None:
    assert _complexity_bin(0) == "trivial"
    assert _complexity_bin(1) == "trivial"
    assert _complexity_bin(2) == "simple"
    assert _complexity_bin(3) == "simple"
    assert _complexity_bin(4) == "moderate"
    assert _complexity_bin(6) == "moderate"
    assert _complexity_bin(7) == "complex"
    assert _complexity_bin(10) == "complex"


def test_fetch_indicators() -> None:
    conn = Dhis2Connection()
    indicators = fetch_indicators.fn(conn, "district")
    assert len(indicators) == 10
    assert all(isinstance(ind, RawIndicator) for ind in indicators)


def test_flatten_indicators() -> None:
    conn = Dhis2Connection()
    raw = fetch_indicators.fn(conn, "district")
    flat = flatten_indicators.fn(raw)
    assert len(flat) == 10
    assert all(isinstance(ind, FlatIndicator) for ind in flat)


def test_indicator_report() -> None:
    conn = Dhis2Connection()
    raw = fetch_indicators.fn(conn, "district")
    flat = flatten_indicators.fn(raw)
    report = indicator_report.fn(flat)
    assert report.total == 10
    assert report.most_complex_name != ""
    assert report.simplest_name != ""


def test_write_csv(tmp_path: Path) -> None:
    conn = Dhis2Connection()
    raw = fetch_indicators.fn(conn, "district")
    flat = flatten_indicators.fn(raw)
    path = write_indicator_csv.fn(flat, str(tmp_path))
    assert path.exists()
    assert path.name == "indicators.csv"


def test_flow_runs(tmp_path: Path) -> None:
    state = dhis2_indicators_flow(output_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
