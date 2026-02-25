"""DHIS2 Authenticated API Pipeline.

Reusable pattern for any authenticated API: API key auth, bearer token auth,
basic auth. Shows how to build a generic API client block that works with
different authentication schemes.

Airflow equivalent: None (general pattern).
Prefect approach:    Block-based auth config, pluggable auth strategies.
"""

from __future__ import annotations

import base64

from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.blocks.core import Block
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Custom Block
# ---------------------------------------------------------------------------


class ApiAuthConfig(Block):
    """Configuration for API authentication.

    Supports api_key, bearer, and basic auth types.
    """

    _block_type_name = "api-auth-config"

    auth_type: str  # "api_key", "bearer", "basic"
    base_url: str


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class AuthHeader(BaseModel):
    """An authentication header with masked value."""

    header_name: str
    header_value: str


class ApiResponse(BaseModel):
    """Simulated API response."""

    endpoint: str
    status_code: int
    record_count: int
    auth_type: str


class AuthReport(BaseModel):
    """Summary of authentication strategy testing."""

    configs_tested: int
    successful: int
    results: list[ApiResponse]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


def _mask_value(value: str) -> str:
    """Mask a credential value for display."""
    if len(value) <= 4:
        return "****"
    return value[:2] + "*" * (len(value) - 4) + value[-2:]


@task
def build_auth_header(config: ApiAuthConfig, credentials: str) -> AuthHeader:
    """Build an HTTP authentication header based on auth type.

    Args:
        config: API auth configuration.
        credentials: Secret credentials (API key, token, or user:pass).

    Returns:
        AuthHeader with masked value.
    """
    if config.auth_type == "api_key":
        header = AuthHeader(
            header_name="X-API-Key",
            header_value=_mask_value(credentials),
        )
    elif config.auth_type == "bearer":
        header = AuthHeader(
            header_name="Authorization",
            header_value=f"Bearer {_mask_value(credentials)}",
        )
    elif config.auth_type == "basic":
        encoded = base64.b64encode(credentials.encode()).decode()
        header = AuthHeader(
            header_name="Authorization",
            header_value=f"Basic {_mask_value(encoded)}",
        )
    else:
        header = AuthHeader(header_name="", header_value="")

    print(f"Built {config.auth_type} auth header: {header.header_name}")
    return header


@task
def authenticated_fetch(config: ApiAuthConfig, credentials: str, endpoint: str) -> ApiResponse:
    """Perform a simulated authenticated API fetch.

    In a real implementation::

        header = build_auth_header.fn(config, credentials)
        response = httpx.get(f"{config.base_url}/{endpoint}",
                             headers={header.header_name: header.header_value})
        return ApiResponse(endpoint=endpoint, status_code=response.status_code, ...)

    Args:
        config: API auth configuration.
        credentials: Secret credentials.
        endpoint: API endpoint path.

    Returns:
        ApiResponse.
    """
    _ = credentials  # consumed by real implementation
    response = ApiResponse(
        endpoint=endpoint,
        status_code=200,
        record_count=10,
        auth_type=config.auth_type,
    )
    print(f"Fetched {endpoint} with {config.auth_type} auth -> {response.status_code}")
    return response


@task
def auth_report(responses: list[ApiResponse]) -> AuthReport:
    """Build a summary report of auth strategy testing.

    Args:
        responses: API responses from each auth strategy.

    Returns:
        AuthReport.
    """
    successful = sum(1 for r in responses if r.status_code == 200)
    return AuthReport(
        configs_tested=len(responses),
        successful=successful,
        results=responses,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="dhis2_authenticated_api", log_prints=True)
def dhis2_authenticated_api_flow() -> AuthReport:
    """Test three authentication strategies against a simulated API.

    Returns:
        AuthReport.
    """
    configs = [
        (ApiAuthConfig(auth_type="api_key", base_url="https://api.example.com"), "my-secret-api-key-123"),
        (ApiAuthConfig(auth_type="bearer", base_url="https://api.example.com"), "eyJhbGciOiJSUzI1NiJ9.token"),
        (ApiAuthConfig(auth_type="basic", base_url="https://api.example.com"), "admin:district"),
    ]

    responses: list[ApiResponse] = []
    for config, creds in configs:
        build_auth_header(config, creds)
        response = authenticated_fetch(config, creds, "api/data")
        responses.append(response)

    report = auth_report(responses)

    lines = ["## Auth Strategy Report", ""]
    lines.append("| Auth Type | Endpoint | Status |")
    lines.append("|-----------|----------|--------|")
    for r in report.results:
        lines.append(f"| {r.auth_type} | {r.endpoint} | {r.status_code} |")
    lines.append(f"\nTested: {report.configs_tested}, Successful: {report.successful}")
    create_markdown_artifact(key="dhis2-auth-report", markdown="\n".join(lines))

    print(f"Auth report: {report.successful}/{report.configs_tested} strategies successful")
    return report


if __name__ == "__main__":
    load_dotenv()
    dhis2_authenticated_api_flow()
