"""Tests for WorldPop Population Stats flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "cloud_worldpop_population_stats",
    Path(__file__).resolve().parent.parent.parent / "flows" / "cloud" / "cloud_worldpop_population_stats.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["cloud_worldpop_population_stats"] = _mod
_spec.loader.exec_module(_mod)

PopulationQuery = _mod.PopulationQuery
PopulationResult = _mod.PopulationResult
PopulationSummary = _mod.PopulationSummary
query_population = _mod.query_population
format_results = _mod.format_results
build_summary = _mod.build_summary
worldpop_population_stats_flow = _mod.worldpop_population_stats_flow

_SAMPLE_GEOJSON = {
    "type": "Polygon",
    "coordinates": [[[38.7, 9.0], [38.8, 9.0], [38.8, 9.1], [38.7, 9.1], [38.7, 9.0]]],
}


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_population_query_defaults() -> None:
    q = PopulationQuery(geojson=_SAMPLE_GEOJSON)
    assert q.year == 2020
    assert q.run_async is False


def test_population_result_model() -> None:
    r = PopulationResult(total_population=1234567.0, year=2020)
    assert r.total_population == 1234567.0
    assert r.source == "worldpop"


def test_population_summary_model() -> None:
    results = [PopulationResult(total_population=1000.0, year=2020)]
    s = PopulationSummary(results=results, total_queries=1, markdown="test")
    assert s.total_queries == 1


# ---------------------------------------------------------------------------
# Task tests
# ---------------------------------------------------------------------------


@patch("cloud_worldpop_population_stats.httpx.Client")
def test_query_population_sync(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "status": "ok",
        "data": {"total_population": 54321.5},
    }
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = query_population.fn(2020, _SAMPLE_GEOJSON, run_async=False)

    assert isinstance(result, PopulationResult)
    assert result.total_population == 54321.5
    assert result.year == 2020


@patch("cloud_worldpop_population_stats._poll_async_result")
@patch("cloud_worldpop_population_stats.httpx.Client")
def test_query_population_async(mock_client_cls: MagicMock, mock_poll: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "status": "created",
        "taskid": "abc123",
    }
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    mock_poll.return_value = {
        "status": "finished",
        "data": {"total_population": 99999.0},
    }

    result = query_population.fn(2020, _SAMPLE_GEOJSON, run_async=True)

    assert result.total_population == 99999.0
    mock_poll.assert_called_once_with("abc123")


def test_format_results() -> None:
    results = [
        PopulationResult(total_population=1234.5, year=2020),
        PopulationResult(total_population=5678.0, year=2019),
    ]
    formatted = format_results.fn(results)
    assert len(formatted) == 2
    assert formatted[0]["year"] == 2020
    assert formatted[0]["total_population"] == "1,234"


def test_build_summary() -> None:
    results = [
        PopulationResult(total_population=1000.0, year=2020),
        PopulationResult(total_population=2000.0, year=2019),
    ]
    summary = build_summary.fn(results)

    assert isinstance(summary, PopulationSummary)
    assert summary.total_queries == 2
    assert "3,000" in summary.markdown
    assert "2020" in summary.markdown


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("cloud_worldpop_population_stats.httpx.Client")
def test_flow_runs(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "status": "ok",
        "data": {"total_population": 12345.0},
    }
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    state = worldpop_population_stats_flow(return_state=True)
    assert state.is_completed()
