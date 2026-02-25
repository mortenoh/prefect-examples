"""Shared DHIS2 helpers -- credentials block, API client, models.

Provides ``Dhis2Credentials`` (custom Block storing connection details) and
``Dhis2Client`` (authenticated API client with methods for common DHIS2
operations).  Used by flows 101--108.

The DHIS2 play server (https://play.im.dhis2.org/dev) is publicly available
with credentials admin/district.
"""

from __future__ import annotations

import re
from typing import Any

import httpx
from prefect.blocks.core import Block
from pydantic import BaseModel, Field, SecretStr

# ---------------------------------------------------------------------------
# API Client
# ---------------------------------------------------------------------------


class Dhis2Client:
    """Authenticated DHIS2 API client.

    Wraps an ``httpx.Client`` scoped to ``/api`` and exposes methods for
    common DHIS2 operations.  Use as a context manager or call ``.close()``
    explicitly.
    """

    def __init__(self, base_url: str, username: str, password: str) -> None:
        self._base_url = base_url
        self._username = username
        self._password = password
        self._http = httpx.Client(
            base_url=f"{base_url}/api",
            auth=(username, password),
            timeout=60,
        )

    def __reduce__(self) -> tuple[type, tuple[str, str, str]]:
        """Allow pickling so Prefect can hash this object for cache keys."""
        return (Dhis2Client, (self._base_url, self._username, self._password))

    def __enter__(self) -> Dhis2Client:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._http.close()

    def get_server_info(self) -> dict[str, Any]:
        """Fetch /api/system/info -- version, revision, etc."""
        resp = self._http.get("/system/info")
        resp.raise_for_status()
        result: dict[str, Any] = resp.json()
        return result

    def fetch_metadata(
        self,
        endpoint: str,
        fields: str = ":owner",
    ) -> list[dict[str, Any]]:
        """Fetch all records from a metadata endpoint.

        Args:
            endpoint: API endpoint name (e.g. "organisationUnits").
            fields: The fields parameter for the DHIS2 API.

        Returns:
            List of metadata records as dicts.
        """
        resp = self._http.get(
            f"/{endpoint}",
            params={"paging": "false", "fields": fields},
        )
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        key = endpoint.split("?")[0]
        result: list[dict[str, Any]] = data[key]
        return result

    def fetch_analytics(
        self,
        dimension: list[str],
        filter_param: str | None = None,
    ) -> dict[str, Any]:
        """Fetch analytics data with dimension parameters.

        Args:
            dimension: List of dimension parameters (e.g. "dx:uid1;uid2").
            filter_param: Optional filter parameter (e.g. "pe:LAST_4_QUARTERS").

        Returns:
            Raw analytics response dict with "headers" and "rows".
        """
        params: dict[str, Any] = {"dimension": dimension}
        if filter_param:
            params["filter"] = filter_param
        resp = self._http.get("/analytics", params=params)
        resp.raise_for_status()
        result: dict[str, Any] = resp.json()
        return result


# ---------------------------------------------------------------------------
# Credentials Block
# ---------------------------------------------------------------------------


class Dhis2Credentials(Block):
    """Credentials block for a DHIS2 instance.

    Equivalent to ``BaseHook.get_connection("dhis2_default")`` in Airflow.
    Stores connection details and returns a ``Dhis2Client`` via
    ``get_client()``.
    """

    _block_type_name = "dhis2-credentials"

    base_url: str = Field(
        default="https://play.im.dhis2.org/dev",
        description="DHIS2 instance base URL",
    )
    username: str = Field(default="admin", description="DHIS2 username")
    password: SecretStr = Field(
        default=SecretStr("district"),
        description="DHIS2 password",
    )

    def get_client(self) -> Dhis2Client:
        """Return an authenticated ``Dhis2Client``."""
        return Dhis2Client(
            self.base_url,
            self.username,
            self.password.get_secret_value(),
        )


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class Dhis2ApiResponse(BaseModel):
    """Wrapper for an API response summary."""

    endpoint: str
    record_count: int
    status_code: int = 200


# ---------------------------------------------------------------------------
# Credentials helpers
# ---------------------------------------------------------------------------

OPERAND_PATTERN = re.compile(r"#\{[^}]+\}")


def get_dhis2_credentials(name: str = "dhis2") -> Dhis2Credentials:
    """Load a DHIS2 credentials block, falling back to inline defaults.

    Args:
        name: Block name to load (default ``"dhis2"``).

    Returns:
        Dhis2Credentials instance.
    """
    try:
        return Dhis2Credentials.load(name)  # type: ignore[return-value]
    except Exception:
        return Dhis2Credentials()
