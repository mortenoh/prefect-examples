"""Tests for flow 043 -- HTTP Tasks."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "flow_043",
    Path(__file__).resolve().parent.parent / "flows" / "043_http_tasks.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_043"] = _mod
_spec.loader.exec_module(_mod)

http_get = _mod.http_get
http_post = _mod.http_post
check_endpoint = _mod.check_endpoint


def _mock_response(json_data: dict, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    resp.is_success = 200 <= status_code < 300
    return resp


@patch("httpx.get")
def test_http_get(mock_get: MagicMock) -> None:
    mock_get.return_value = _mock_response({"origin": "1.2.3.4"})
    result = http_get.fn("https://httpbin.org/get")
    assert isinstance(result, dict)
    assert result["origin"] == "1.2.3.4"


@patch("httpx.post")
def test_http_post(mock_post: MagicMock) -> None:
    mock_post.return_value = _mock_response({"json": {"key": "value"}})
    result = http_post.fn("https://httpbin.org/post", {"key": "value"})
    assert isinstance(result, dict)
    assert result["json"]["key"] == "value"


@patch("httpx.get")
def test_check_endpoint_healthy(mock_get: MagicMock) -> None:
    mock_get.return_value = _mock_response({}, 200)
    result = check_endpoint.fn("https://httpbin.org/status/200")
    assert result is True


@patch("httpx.get")
def test_check_endpoint_unhealthy(mock_get: MagicMock) -> None:
    mock_get.return_value = _mock_response({}, 500)
    result = check_endpoint.fn("https://httpbin.org/status/500")
    assert result is False
