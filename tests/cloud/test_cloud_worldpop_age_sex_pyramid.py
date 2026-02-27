"""Tests for WorldPop Age-Sex Pyramid flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "cloud_worldpop_age_sex_pyramid",
    Path(__file__).resolve().parent.parent.parent / "flows" / "cloud" / "cloud_worldpop_age_sex_pyramid.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["cloud_worldpop_age_sex_pyramid"] = _mod
_spec.loader.exec_module(_mod)

DemographicQuery = _mod.DemographicQuery
AgeSexBracket = _mod.AgeSexBracket
DemographicStats = _mod.DemographicStats
PyramidReport = _mod.PyramidReport
query_age_sex = _mod.query_age_sex
parse_pyramid = _mod.parse_pyramid
compute_demographics = _mod.compute_demographics
build_pyramid_report = _mod.build_pyramid_report
worldpop_age_sex_pyramid_flow = _mod.worldpop_age_sex_pyramid_flow

_SAMPLE_GEOJSON = {
    "type": "Polygon",
    "coordinates": [[[38.7, 9.0], [38.8, 9.0], [38.8, 9.1], [38.7, 9.1], [38.7, 9.0]]],
}


def _make_pyramid_response() -> dict[str, object]:
    """Build a canned age-sex pyramid API response."""
    pyramid: dict[str, float] = {}
    for i in range(17):
        pyramid[f"M_{i}"] = 100.0 + i * 10
        pyramid[f"F_{i}"] = 110.0 + i * 10
    return {"data": {"agesexpyramid": pyramid}}


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_demographic_query_defaults() -> None:
    q = DemographicQuery(geojson=_SAMPLE_GEOJSON)
    assert q.year == 2020


def test_age_sex_bracket_model() -> None:
    b = AgeSexBracket(age_group="0-4", male=100.0, female=110.0, total=210.0)
    assert b.total == 210.0


def test_demographic_stats_model() -> None:
    s = DemographicStats(
        total_population=1000.0,
        total_male=480.0,
        total_female=520.0,
        sex_ratio=92.3,
        dependency_ratio=75.0,
        median_age_bracket="20-24",
    )
    assert s.sex_ratio == 92.3


# ---------------------------------------------------------------------------
# Task tests
# ---------------------------------------------------------------------------


@patch("cloud_worldpop_age_sex_pyramid.httpx.Client")
def test_query_age_sex(mock_client_cls: MagicMock) -> None:
    response_data = _make_pyramid_response()
    mock_resp = MagicMock()
    mock_resp.json.return_value = response_data
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = query_age_sex.fn(2020, _SAMPLE_GEOJSON)
    assert "data" in result
    assert "agesexpyramid" in result["data"]


def test_parse_pyramid() -> None:
    response = _make_pyramid_response()
    brackets = parse_pyramid.fn(response)

    assert len(brackets) == 17
    assert brackets[0].age_group == "0-4"
    assert brackets[0].male == 100.0
    assert brackets[0].female == 110.0
    assert brackets[0].total == 210.0
    assert brackets[-1].age_group == "80+"


def test_parse_pyramid_empty() -> None:
    response: dict[str, object] = {"data": {"agesexpyramid": {}}}
    brackets = parse_pyramid.fn(response)

    assert len(brackets) == 17
    for b in brackets:
        assert b.total == 0.0


def test_compute_demographics() -> None:
    response = _make_pyramid_response()
    brackets = parse_pyramid.fn(response)
    stats = compute_demographics.fn(brackets)

    assert isinstance(stats, DemographicStats)
    assert stats.total_population > 0
    assert stats.total_male > 0
    assert stats.total_female > 0
    assert stats.sex_ratio > 0
    assert stats.dependency_ratio > 0
    assert stats.median_age_bracket != ""


def test_compute_demographics_sex_ratio() -> None:
    brackets = [
        AgeSexBracket(age_group="20-24", male=100.0, female=200.0, total=300.0),
    ]
    # Only one bracket: all are working age (index 3-12), but our bracket
    # starts at 0 so it's young (index 0)
    stats = compute_demographics.fn(brackets)
    assert stats.sex_ratio == 50.0  # 100/200 * 100


def test_build_pyramid_report() -> None:
    brackets = [
        AgeSexBracket(age_group="0-4", male=100.0, female=110.0, total=210.0),
        AgeSexBracket(age_group="5-9", male=90.0, female=95.0, total=185.0),
    ]
    stats = DemographicStats(
        total_population=395.0,
        total_male=190.0,
        total_female=205.0,
        sex_ratio=92.7,
        dependency_ratio=80.0,
        median_age_bracket="0-4",
    )
    report = build_pyramid_report.fn(2020, brackets, stats)

    assert isinstance(report, PyramidReport)
    assert report.year == 2020
    assert len(report.brackets) == 2
    assert "0-4" in report.markdown
    assert "92.7" in report.markdown


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("cloud_worldpop_age_sex_pyramid.httpx.Client")
def test_flow_runs(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = _make_pyramid_response()
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    state = worldpop_age_sex_pyramid_flow(return_state=True)
    assert state.is_completed()
