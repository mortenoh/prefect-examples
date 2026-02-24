"""Tests for flow 107 -- DHIS2 Analytics Query."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_107",
    Path(__file__).resolve().parent.parent / "flows" / "107_dhis2_analytics.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_107"] = _mod
_spec.loader.exec_module(_mod)

Dhis2Connection = _mod.Dhis2Connection
RawAnalyticsResponse = _mod.RawAnalyticsResponse
AnalyticsQuery = _mod.AnalyticsQuery
AnalyticsRow = _mod.AnalyticsRow
AnalyticsReport = _mod.AnalyticsReport
build_query = _mod.build_query
fetch_analytics = _mod.fetch_analytics
parse_analytics = _mod.parse_analytics
write_analytics_csv = _mod.write_analytics_csv
analytics_report = _mod.analytics_report
dhis2_analytics_flow = _mod.dhis2_analytics_flow


def test_build_query_defaults() -> None:
    query = build_query.fn()
    assert len(query.data_elements) == 3
    assert len(query.org_units) == 2
    assert len(query.periods) == 3


def test_build_query_custom() -> None:
    query = build_query.fn(data_elements=["DE_X"], org_units=["OU_Y"], periods=["202401"])
    assert query.data_elements == ["DE_X"]
    assert query.org_units == ["OU_Y"]
    assert query.periods == ["202401"]


def test_fetch_analytics() -> None:
    conn = Dhis2Connection()
    query = build_query.fn()
    response = fetch_analytics.fn(conn, "district", query)
    assert isinstance(response, RawAnalyticsResponse)
    expected_rows = len(query.data_elements) * len(query.org_units) * len(query.periods)
    assert len(response.rows) == expected_rows


def test_parse_analytics() -> None:
    conn = Dhis2Connection()
    query = build_query.fn()
    response = fetch_analytics.fn(conn, "district", query)
    rows = parse_analytics.fn(response)
    assert len(rows) == len(response.rows)
    assert all(isinstance(r, AnalyticsRow) for r in rows)


def test_value_range() -> None:
    conn = Dhis2Connection()
    query = build_query.fn()
    response = fetch_analytics.fn(conn, "district", query)
    rows = parse_analytics.fn(response)
    report = analytics_report.fn(rows, query)
    assert report.value_range[0] <= report.value_range[1]
    assert report.value_range[0] > 0


def test_data_element_counts() -> None:
    conn = Dhis2Connection()
    query = build_query.fn()
    response = fetch_analytics.fn(conn, "district", query)
    rows = parse_analytics.fn(response)
    report = analytics_report.fn(rows, query)
    assert len(report.data_element_counts) == len(query.data_elements)


def test_write_csv(tmp_path: Path) -> None:
    conn = Dhis2Connection()
    query = build_query.fn()
    response = fetch_analytics.fn(conn, "district", query)
    rows = parse_analytics.fn(response)
    path = write_analytics_csv.fn(rows, str(tmp_path))
    assert path.exists()
    assert path.name == "analytics.csv"


def test_flow_runs(tmp_path: Path) -> None:
    state = dhis2_analytics_flow(output_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
