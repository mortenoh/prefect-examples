"""Tests for flow 111 -- DHIS2 Deployment."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "flow_111",
    Path(__file__).resolve().parent.parent / "flows" / "111_dhis2_deployment.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_111"] = _mod
_spec.loader.exec_module(_mod)

Dhis2Connection = _mod.Dhis2Connection
ServerInfo = _mod.ServerInfo
EndpointSyncResult = _mod.EndpointSyncResult
SyncReport = _mod.SyncReport
connect_and_verify = _mod.connect_and_verify
sync_endpoint = _mod.sync_endpoint
build_sync_summary = _mod.build_sync_summary
dhis2_deployment_flow = _mod.dhis2_deployment_flow


def _mock_client(json_data: dict) -> MagicMock:
    """Create a mock httpx client returning *json_data* for every GET."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = json_data
    mock_resp.raise_for_status.return_value = None

    mock_cl = MagicMock()
    mock_cl.__enter__ = MagicMock(return_value=mock_cl)
    mock_cl.__exit__ = MagicMock(return_value=False)
    mock_cl.get.return_value = mock_resp
    return mock_cl


def _mock_client_with_side_effect() -> MagicMock:
    """Create a mock client that dispatches based on the URL path."""

    def _get_side_effect(url: str, **kwargs: object) -> MagicMock:
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        if "system/info" in url:
            resp.json.return_value = {"version": "2.43-SNAPSHOT"}
        elif "organisationUnits" in url:
            resp.json.return_value = {"organisationUnits": [{"id": "OU1"}, {"id": "OU2"}]}
        elif "dataElements" in url:
            resp.json.return_value = {"dataElements": [{"id": "DE1"}]}
        elif "indicators" in url:
            resp.json.return_value = {"indicators": [{"id": "IND1"}, {"id": "IND2"}, {"id": "IND3"}]}
        else:
            resp.json.return_value = {}
        return resp

    mock_cl = MagicMock()
    mock_cl.__enter__ = MagicMock(return_value=mock_cl)
    mock_cl.__exit__ = MagicMock(return_value=False)
    mock_cl.get.side_effect = _get_side_effect
    return mock_cl


@patch.object(Dhis2Connection, "get_client")
def test_connect_and_verify(mock_get_client: MagicMock) -> None:
    mock_get_client.return_value = _mock_client({"version": "2.43-SNAPSHOT", "revision": "abc123"})
    conn = Dhis2Connection()
    info = connect_and_verify.fn(conn)
    assert isinstance(info, ServerInfo)
    assert info.version == "2.43-SNAPSHOT"
    assert info.revision == "abc123"


@patch.object(Dhis2Connection, "get_client")
def test_sync_endpoint(mock_get_client: MagicMock) -> None:
    mock_get_client.return_value = _mock_client({"organisationUnits": [{"id": "OU1"}, {"id": "OU2"}]})
    conn = Dhis2Connection()
    result = sync_endpoint.fn(conn, "organisationUnits")
    assert isinstance(result, EndpointSyncResult)
    assert result.endpoint == "organisationUnits"
    assert result.record_count == 2


def test_build_sync_summary() -> None:
    server_info = ServerInfo(version="2.43")
    results = [
        EndpointSyncResult(endpoint="organisationUnits", record_count=10),
        EndpointSyncResult(endpoint="dataElements", record_count=5),
    ]
    report = build_sync_summary.fn(server_info, results)
    assert isinstance(report, SyncReport)
    assert report.total_records == 15
    assert report.server_version == "2.43"
    assert "organisationUnits" in report.summary_markdown
    assert "dataElements" in report.summary_markdown
    assert "**Total:** 15 records" in report.summary_markdown


@patch.object(Dhis2Connection, "get_client")
def test_flow_default_endpoints(mock_get_client: MagicMock) -> None:
    mock_get_client.return_value = _mock_client_with_side_effect()
    report = dhis2_deployment_flow()
    assert isinstance(report, SyncReport)
    assert len(report.endpoint_results) == 3
    endpoints = [r.endpoint for r in report.endpoint_results]
    assert "organisationUnits" in endpoints
    assert "dataElements" in endpoints
    assert "indicators" in endpoints


@patch.object(Dhis2Connection, "get_client")
def test_flow_custom_endpoints(mock_get_client: MagicMock) -> None:
    mock_get_client.return_value = _mock_client_with_side_effect()
    report = dhis2_deployment_flow(endpoints=["organisationUnits", "dataElements"])
    assert isinstance(report, SyncReport)
    assert len(report.endpoint_results) == 2
    endpoints = [r.endpoint for r in report.endpoint_results]
    assert "organisationUnits" in endpoints
    assert "dataElements" in endpoints
    assert "indicators" not in endpoints


@patch.object(Dhis2Connection, "get_client")
def test_flow_runs(mock_get_client: MagicMock) -> None:
    mock_get_client.return_value = _mock_client_with_side_effect()
    state = dhis2_deployment_flow(return_state=True)
    assert state.is_completed()
