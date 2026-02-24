"""Tests for the dhis2_connection deployment flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "deploy_dhis2_connection",
    Path(__file__).resolve().parent.parent
    / "deployments"
    / "dhis2_connection"
    / "flow.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["deploy_dhis2_connection"] = _mod
_spec.loader.exec_module(_mod)

Dhis2Client = _mod.Dhis2Client
Dhis2Credentials = _mod.Dhis2Credentials
ConnectionReport = _mod.ConnectionReport
verify_connection = _mod.verify_connection
fetch_org_unit_count = _mod.fetch_org_unit_count
build_report = _mod.build_report
dhis2_connection_flow = _mod.dhis2_connection_flow


def _mock_client_with_side_effect() -> MagicMock:
    """Create a mock Dhis2Client that dispatches based on the method."""
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.get_server_info.return_value = {"version": "2.43-SNAPSHOT"}
    mock_client.fetch_metadata.return_value = [{"id": "OU1"}, {"id": "OU2"}]
    return mock_client


@patch.object(Dhis2Client, "get_server_info")
def test_verify_connection(mock_info: MagicMock) -> None:
    mock_info.return_value = {"version": "2.43-SNAPSHOT", "revision": "abc123"}
    client = MagicMock(spec=Dhis2Client)
    client.get_server_info = mock_info
    result = verify_connection.fn(client)
    assert result["version"] == "2.43-SNAPSHOT"


@patch.object(Dhis2Client, "fetch_metadata")
def test_fetch_org_unit_count(mock_fetch: MagicMock) -> None:
    mock_fetch.return_value = [{"id": "OU1"}, {"id": "OU2"}, {"id": "OU3"}]
    client = MagicMock(spec=Dhis2Client)
    client.fetch_metadata = mock_fetch
    count = fetch_org_unit_count.fn(client)
    assert count == 3


def test_build_report() -> None:
    creds = Dhis2Credentials()
    server_info = {"version": "2.43"}
    report = build_report.fn(creds, server_info, 5)
    assert isinstance(report, ConnectionReport)
    assert report.server_version == "2.43"
    assert report.org_unit_count == 5
    assert report.host == creds.base_url
    assert report.username == creds.username


@patch.object(Dhis2Credentials, "get_client")
def test_flow_runs(mock_get_client: MagicMock) -> None:
    mock_get_client.return_value = _mock_client_with_side_effect()
    state = dhis2_connection_flow(return_state=True)
    assert state.is_completed()
    report = state.result()
    assert isinstance(report, ConnectionReport)
    assert report.server_version == "2.43-SNAPSHOT"
    assert report.org_unit_count == 2
