"""Tests for flow 107 -- DHIS2 Analytics Query."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "flow_107",
    Path(__file__).resolve().parent.parent / "flows" / "107_dhis2_analytics.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_107"] = _mod
_spec.loader.exec_module(_mod)

Dhis2Connection = _mod.Dhis2Connection
AnalyticsQuery = _mod.AnalyticsQuery
AnalyticsRow = _mod.AnalyticsRow
AnalyticsReport = _mod.AnalyticsReport
build_query = _mod.build_query
fetch_analytics = _mod.fetch_analytics
parse_analytics = _mod.parse_analytics
write_analytics_csv = _mod.write_analytics_csv
analytics_report = _mod.analytics_report
dhis2_analytics_flow = _mod.dhis2_analytics_flow

SAMPLE_ANALYTICS_RESPONSE = {
    "headers": [
        {"name": "dx", "column": "Data"},
        {"name": "ou", "column": "Organisation unit"},
        {"name": "pe", "column": "Period"},
        {"name": "value", "column": "Value"},
    ],
    "rows": [
        ["fbfJHSPpUQD", "ImspTQPwCqd", "2024Q1", "1234.5"],
        ["fbfJHSPpUQD", "ImspTQPwCqd", "2024Q2", "2345.6"],
        ["cYeuwXTCPkU", "ImspTQPwCqd", "2024Q1", "567.8"],
        ["cYeuwXTCPkU", "ImspTQPwCqd", "2024Q2", "890.1"],
    ],
}


def _mock_client(json_data: dict) -> MagicMock:
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = json_data
    mock_resp.raise_for_status.return_value = None

    mock_cl = MagicMock()
    mock_cl.__enter__ = MagicMock(return_value=mock_cl)
    mock_cl.__exit__ = MagicMock(return_value=False)
    mock_cl.get.return_value = mock_resp
    return mock_cl


def test_build_query_defaults() -> None:
    query = build_query.fn()
    assert len(query.dimension) == 2
    assert any("dx:" in d for d in query.dimension)
    assert any("ou:" in d for d in query.dimension)
    assert query.filter_param == "pe:LAST_4_QUARTERS"


def test_build_query_custom() -> None:
    query = build_query.fn(data_elements="DE_X", org_units="OU_Y", filter_param="pe:2024Q1")
    assert "dx:DE_X" in query.dimension
    assert "ou:OU_Y" in query.dimension
    assert query.filter_param == "pe:2024Q1"


@patch.object(Dhis2Connection, "get_client")
def test_fetch_analytics(mock_get_client: MagicMock) -> None:
    mock_get_client.return_value = _mock_client(SAMPLE_ANALYTICS_RESPONSE)
    conn = Dhis2Connection()
    query = build_query.fn()
    response = fetch_analytics.fn(conn, query)
    assert "headers" in response
    assert "rows" in response
    assert len(response["rows"]) == 4


def test_parse_analytics() -> None:
    rows = parse_analytics.fn(SAMPLE_ANALYTICS_RESPONSE)
    assert len(rows) == 4
    assert all(isinstance(r, AnalyticsRow) for r in rows)
    assert rows[0].dx == "fbfJHSPpUQD"
    assert rows[0].value == 1234.5


def test_value_range() -> None:
    rows = parse_analytics.fn(SAMPLE_ANALYTICS_RESPONSE)
    query = build_query.fn()
    report = analytics_report.fn(rows, query)
    assert report.value_range[0] <= report.value_range[1]
    assert report.value_range[0] == 567.8
    assert report.value_range[1] == 2345.6


def test_data_element_counts() -> None:
    rows = parse_analytics.fn(SAMPLE_ANALYTICS_RESPONSE)
    query = build_query.fn()
    report = analytics_report.fn(rows, query)
    assert report.data_element_counts["fbfJHSPpUQD"] == 2
    assert report.data_element_counts["cYeuwXTCPkU"] == 2


def test_write_csv(tmp_path: Path) -> None:
    rows = parse_analytics.fn(SAMPLE_ANALYTICS_RESPONSE)
    path = write_analytics_csv.fn(rows, str(tmp_path))
    assert path.exists()
    assert path.name == "analytics.csv"


@patch.object(Dhis2Connection, "get_client")
def test_flow_runs(mock_get_client: MagicMock, tmp_path: Path) -> None:
    mock_get_client.return_value = _mock_client(SAMPLE_ANALYTICS_RESPONSE)
    state = dhis2_analytics_flow(output_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
