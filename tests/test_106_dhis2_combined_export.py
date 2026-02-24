"""Tests for flow 106 -- DHIS2 Combined Export."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "flow_106",
    Path(__file__).resolve().parent.parent / "flows" / "106_dhis2_combined_export.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_106"] = _mod
_spec.loader.exec_module(_mod)

Dhis2Connection = _mod.Dhis2Connection
ExportResult = _mod.ExportResult
CombinedExportReport = _mod.CombinedExportReport
export_org_units = _mod.export_org_units
export_data_elements = _mod.export_data_elements
export_indicators = _mod.export_indicators
combined_report = _mod.combined_report
dhis2_combined_export_flow = _mod.dhis2_combined_export_flow

SAMPLE_OU = [{"id": "OU1", "name": "A", "level": 1}]
SAMPLE_DE = [{"id": "DE1", "name": "B", "valueType": "NUMBER"}]
SAMPLE_IND = [{"id": "IND1", "name": "C", "numerator": "#{a.b}", "denominator": "1"}]


def _mock_response(json_data: dict, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


@patch("httpx.get")
def test_export_org_units(mock_get: MagicMock, tmp_path: Path) -> None:
    mock_get.return_value = _mock_response({"organisationUnits": SAMPLE_OU})
    conn = Dhis2Connection()
    result = export_org_units.fn(conn, "district", str(tmp_path))
    assert isinstance(result, ExportResult)
    assert result.endpoint == "organisationUnits"
    assert result.format == "csv"
    assert Path(result.output_path).exists()


@patch("httpx.get")
def test_export_data_elements(mock_get: MagicMock, tmp_path: Path) -> None:
    mock_get.return_value = _mock_response({"dataElements": SAMPLE_DE})
    conn = Dhis2Connection()
    result = export_data_elements.fn(conn, "district", str(tmp_path))
    assert isinstance(result, ExportResult)
    assert result.endpoint == "dataElements"
    assert result.format == "json"
    assert Path(result.output_path).exists()


@patch("httpx.get")
def test_export_indicators(mock_get: MagicMock, tmp_path: Path) -> None:
    mock_get.return_value = _mock_response({"indicators": SAMPLE_IND})
    conn = Dhis2Connection()
    result = export_indicators.fn(conn, "district", str(tmp_path))
    assert isinstance(result, ExportResult)
    assert result.endpoint == "indicators"
    assert result.format == "csv"
    assert Path(result.output_path).exists()


def test_combined_report() -> None:
    results = [
        ExportResult(endpoint="a", record_count=10, output_path="/tmp/a.csv", format="csv"),
        ExportResult(endpoint="b", record_count=20, output_path="/tmp/b.json", format="json"),
        ExportResult(endpoint="c", record_count=5, output_path="/tmp/c.csv", format="csv"),
    ]
    report = combined_report.fn(results)
    assert report.total_records == 35
    assert report.format_counts["csv"] == 2
    assert report.format_counts["json"] == 1


@patch("httpx.get")
def test_format_counts(mock_get: MagicMock, tmp_path: Path) -> None:
    def side_effect(url: str, **kwargs: object) -> MagicMock:
        if "organisationUnits" in url:
            return _mock_response({"organisationUnits": SAMPLE_OU})
        if "dataElements" in url:
            return _mock_response({"dataElements": SAMPLE_DE})
        return _mock_response({"indicators": SAMPLE_IND})

    mock_get.side_effect = side_effect
    conn = Dhis2Connection()
    r1 = export_org_units.fn(conn, "district", str(tmp_path))
    r2 = export_data_elements.fn(conn, "district", str(tmp_path))
    r3 = export_indicators.fn(conn, "district", str(tmp_path))
    report = combined_report.fn([r1, r2, r3])
    assert report.format_counts["csv"] == 2
    assert report.format_counts["json"] == 1


@patch("httpx.get")
def test_flow_runs(mock_get: MagicMock, tmp_path: Path) -> None:
    def side_effect(url: str, **kwargs: object) -> MagicMock:
        if "organisationUnits" in url:
            return _mock_response({"organisationUnits": SAMPLE_OU})
        if "dataElements" in url:
            return _mock_response({"dataElements": SAMPLE_DE})
        return _mock_response({"indicators": SAMPLE_IND})

    mock_get.side_effect = side_effect
    state = dhis2_combined_export_flow(output_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
