"""Tests for flow 104 -- DHIS2 Indicators API."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "dhis2_indicators",
    Path(__file__).resolve().parent.parent.parent / "flows" / "dhis2" / "dhis2_indicators.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["dhis2_indicators"] = _mod
_spec.loader.exec_module(_mod)

from prefect_examples.dhis2 import Dhis2Client, Dhis2Credentials  # noqa: E402

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

SAMPLE_INDICATORS = [
    {
        "id": "IND001",
        "name": "ANC Coverage",
        "shortName": "ANC",
        "indicatorType": {"id": "IT001", "name": "Percentage"},
        "numerator": "#{abc.def}+#{ghi.jkl}",
        "denominator": "#{xyz.uvw}",
    },
    {
        "id": "IND002",
        "name": "Simple Rate",
        "shortName": "SR",
        "indicatorType": {"id": "IT002", "name": "Rate"},
        "numerator": "#{abc.def}",
        "denominator": "1",
    },
    {
        "id": "IND003",
        "name": "Complex Calc",
        "shortName": "CC",
        "indicatorType": {"id": "IT001", "name": "Percentage"},
        "numerator": "#{a.b}+#{c.d}-#{e.f}*#{g.h}",
        "denominator": "#{i.j}+#{k.l}",
    },
]


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


def test_flatten_indicators() -> None:
    flat = flatten_indicators.fn(SAMPLE_INDICATORS)
    assert len(flat) == 3
    assert all(isinstance(ind, FlatIndicator) for ind in flat)


def test_operand_counting() -> None:
    flat = flatten_indicators.fn(SAMPLE_INDICATORS)
    anc = next(i for i in flat if i.id == "IND001")
    assert anc.numerator_operands == 2
    assert anc.denominator_operands == 1


def test_indicator_report() -> None:
    flat = flatten_indicators.fn(SAMPLE_INDICATORS)
    report = indicator_report.fn(flat)
    assert report.total == 3
    assert report.most_complex_name != ""
    assert report.simplest_name != ""


def test_write_csv(tmp_path: Path) -> None:
    flat = flatten_indicators.fn(SAMPLE_INDICATORS)
    path = write_indicator_csv.fn(flat, str(tmp_path))
    assert path.exists()
    assert path.name == "indicators.csv"


@patch.object(Dhis2Credentials, "get_client")
def test_flow_runs(mock_get_client: MagicMock, tmp_path: Path) -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_INDICATORS
    mock_get_client.return_value = mock_client
    state = dhis2_indicators_flow(output_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
