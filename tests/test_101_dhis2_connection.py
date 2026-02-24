"""Tests for flow 101 -- DHIS2 Connection Block."""

import importlib.util
import sys
from pathlib import Path

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
display_connection = _mod.display_connection
dhis2_connection_flow = _mod.dhis2_connection_flow


def test_connection_construction() -> None:
    conn = Dhis2Connection(base_url="https://test.dhis2.org", username="user", api_version="40")
    assert conn.base_url == "https://test.dhis2.org"
    assert conn.username == "user"
    assert conn.api_version == "40"


def test_connection_defaults() -> None:
    conn = Dhis2Connection()
    assert conn.base_url == "https://play.dhis2.org/40"
    assert conn.username == "admin"


def test_password_masking() -> None:
    conn = Dhis2Connection()
    info = get_connection_info.fn(conn, "district")
    assert info.masked_password == "d******t"
    assert "district" not in info.masked_password


def test_short_password_masking() -> None:
    conn = Dhis2Connection()
    info = get_connection_info.fn(conn, "ab")
    assert info.masked_password == "***"


def test_verify_response() -> None:
    conn = Dhis2Connection()
    response = verify_connection.fn(conn, "district")
    assert isinstance(response, Dhis2ApiResponse)
    assert response.status_code == 200
    assert response.endpoint == "system/info"


def test_display_connection() -> None:
    conn = Dhis2Connection()
    info = get_connection_info.fn(conn, "district")
    summary = display_connection.fn(info)
    assert "admin" in summary
    assert "dhis2" in summary


def test_flow_runs() -> None:
    state = dhis2_connection_flow(return_state=True)
    assert state.is_completed()
