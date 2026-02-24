"""Tests for flow 101 -- DHIS2 Connection Block."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "flow_101",
    Path(__file__).resolve().parent.parent / "flows" / "101_dhis2_connection.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_101"] = _mod
_spec.loader.exec_module(_mod)

Dhis2Connection = _mod.Dhis2Connection
Dhis2ApiResponse = _mod.Dhis2ApiResponse
ConnectionInfo = _mod.ConnectionInfo
get_connection_info = _mod.get_connection_info
verify_connection = _mod.verify_connection
fetch_org_unit_count = _mod.fetch_org_unit_count
display_connection = _mod.display_connection
dhis2_connection_flow = _mod.dhis2_connection_flow


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


def test_connection_construction() -> None:
    conn = Dhis2Connection(base_url="https://test.dhis2.org", username="user")
    assert conn.base_url == "https://test.dhis2.org"
    assert conn.username == "user"


def test_connection_defaults() -> None:
    conn = Dhis2Connection()
    assert conn.base_url == "https://play.im.dhis2.org/dev"
    assert conn.username == "admin"


def test_connection_info() -> None:
    conn = Dhis2Connection()
    info = get_connection_info.fn(conn)
    assert info.has_password is True
    assert info.username == "admin"


@patch.object(Dhis2Connection, "get_client")
def test_verify_response(mock_get_client: MagicMock) -> None:
    mock_get_client.return_value = _mock_client({"version": "2.43-SNAPSHOT"})
    conn = Dhis2Connection()
    response = verify_connection.fn(conn)
    assert isinstance(response, Dhis2ApiResponse)
    assert response.endpoint == "system/info"


@patch.object(Dhis2Connection, "get_client")
def test_fetch_org_unit_count(mock_get_client: MagicMock) -> None:
    mock_get_client.return_value = _mock_client({"organisationUnits": [{"id": "a"}, {"id": "b"}]})
    conn = Dhis2Connection()
    count = fetch_org_unit_count.fn(conn)
    assert count == 2


def test_display_connection() -> None:
    conn = Dhis2Connection()
    info = get_connection_info.fn(conn)
    summary = display_connection.fn(info, 100)
    assert "admin" in summary
    assert "dhis2" in summary
    assert "100" in summary


@patch.object(Dhis2Connection, "get_client")
def test_flow_runs(mock_get_client: MagicMock) -> None:
    mock_get_client.return_value = _mock_client({"version": "2.40.0", "organisationUnits": [{"id": "a"}, {"id": "b"}]})
    state = dhis2_connection_flow(return_state=True)
    assert state.is_completed()
