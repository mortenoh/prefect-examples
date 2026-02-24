"""Tests for flow 110 -- DHIS2 Authenticated API Pipeline."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_110",
    Path(__file__).resolve().parent.parent / "flows" / "110_dhis2_authenticated_api.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_110"] = _mod
_spec.loader.exec_module(_mod)

ApiAuthConfig = _mod.ApiAuthConfig
AuthHeader = _mod.AuthHeader
ApiResponse = _mod.ApiResponse
AuthReport = _mod.AuthReport
build_auth_header = _mod.build_auth_header
authenticated_fetch = _mod.authenticated_fetch
auth_report = _mod.auth_report
dhis2_authenticated_api_flow = _mod.dhis2_authenticated_api_flow
_mask_value = _mod._mask_value


def test_mask_short_value() -> None:
    assert _mask_value("abc") == "****"


def test_mask_long_value() -> None:
    masked = _mask_value("my-secret-key")
    assert masked.startswith("my")
    assert masked.endswith("ey")
    assert "*" in masked


def test_api_key_header() -> None:
    config = ApiAuthConfig(auth_type="api_key", base_url="https://api.example.com")
    header = build_auth_header.fn(config, "my-api-key-12345")
    assert header.header_name == "X-API-Key"
    assert "*" in header.header_value


def test_bearer_header() -> None:
    config = ApiAuthConfig(auth_type="bearer", base_url="https://api.example.com")
    header = build_auth_header.fn(config, "eyJtoken123")
    assert header.header_name == "Authorization"
    assert header.header_value.startswith("Bearer ")


def test_basic_header() -> None:
    config = ApiAuthConfig(auth_type="basic", base_url="https://api.example.com")
    header = build_auth_header.fn(config, "admin:password")
    assert header.header_name == "Authorization"
    assert header.header_value.startswith("Basic ")


def test_authenticated_fetch() -> None:
    config = ApiAuthConfig(auth_type="api_key", base_url="https://api.example.com")
    response = authenticated_fetch.fn(config, "test-key", "api/data")
    assert isinstance(response, ApiResponse)
    assert response.status_code == 200
    assert response.auth_type == "api_key"


def test_auth_report() -> None:
    responses = [
        ApiResponse(endpoint="a", status_code=200, record_count=10, auth_type="api_key"),
        ApiResponse(endpoint="b", status_code=200, record_count=5, auth_type="bearer"),
        ApiResponse(endpoint="c", status_code=200, record_count=8, auth_type="basic"),
    ]
    report = auth_report.fn(responses)
    assert report.configs_tested == 3
    assert report.successful == 3


def test_flow_runs() -> None:
    state = dhis2_authenticated_api_flow(return_state=True)
    assert state.is_completed()
