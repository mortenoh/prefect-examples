"""Tests for DHIS2 Block-Based Connection with multi-instance support."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "dhis2_block_connection",
    Path(__file__).resolve().parent.parent.parent / "flows" / "dhis2" / "dhis2_block_connection.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["dhis2_block_connection"] = _mod
_spec.loader.exec_module(_mod)

from prefect_examples.dhis2 import Dhis2ServerInfo  # noqa: E402

Dhis2Client = _mod.Dhis2Client
Dhis2Credentials = _mod.Dhis2Credentials
Dhis2ApiResponse = _mod.Dhis2ApiResponse
ConnectionInfo = _mod.ConnectionInfo
get_connection_info = _mod.get_connection_info
verify_connection = _mod.verify_connection
fetch_org_unit_count = _mod.fetch_org_unit_count
display_connection = _mod.display_connection
dhis2_block_connection_flow = _mod.dhis2_block_connection_flow


def test_connection_info_includes_instance() -> None:
    conn = Dhis2Credentials()
    info = get_connection_info.fn(conn, "my-instance")
    assert info.instance == "my-instance"
    assert info.has_password is True
    assert info.username == "admin"


@patch.object(Dhis2Client, "get_server_info")
def test_verify_response(mock_info: MagicMock) -> None:
    mock_info.return_value = Dhis2ServerInfo(version="2.43-SNAPSHOT")
    client = MagicMock(spec=Dhis2Client)
    client.get_server_info = mock_info
    response = verify_connection.fn(client, "https://play.im.dhis2.org/dev")
    assert isinstance(response, Dhis2ApiResponse)
    assert response.endpoint == "system/info"


@patch.object(Dhis2Client, "fetch_metadata")
def test_fetch_org_unit_count(mock_fetch: MagicMock) -> None:
    mock_fetch.return_value = [{"id": "a"}, {"id": "b"}]
    client = MagicMock(spec=Dhis2Client)
    client.fetch_metadata = mock_fetch
    count = fetch_org_unit_count.fn(client)
    assert count == 2


def test_display_connection_includes_instance() -> None:
    conn = Dhis2Credentials()
    info = get_connection_info.fn(conn, "staging")
    summary = display_connection.fn(info, 100)
    assert "staging" in summary
    assert "admin" in summary
    assert "100" in summary


@patch("dhis2_block_connection.get_dhis2_credentials")
@patch.object(Dhis2Credentials, "get_client")
def test_flow_passes_instance(
    mock_get_client: MagicMock,
    mock_get_creds: MagicMock,
) -> None:
    creds = Dhis2Credentials()
    mock_get_creds.return_value = creds
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.get_server_info.return_value = Dhis2ServerInfo(version="2.40.0")
    mock_client.fetch_metadata.return_value = [{"id": "a"}, {"id": "b"}]
    mock_get_client.return_value = mock_client

    state = dhis2_block_connection_flow(instance="my-block", return_state=True)
    assert state.is_completed()
    mock_get_creds.assert_called_once_with("my-block")
