"""Shared DHIS2 helpers -- block with methods, models, and API client.

Provides the ``Dhis2Connection`` custom block with built-in authentication
and methods for common DHIS2 API operations.  Used by flows 101--108.

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
# Custom Block
# ---------------------------------------------------------------------------


class Dhis2Connection(Block):
    """Connection block for a DHIS2 instance.

    Equivalent to ``BaseHook.get_connection("dhis2_default")`` in Airflow.
    Stores credentials and exposes methods for common API operations.
    """

    _block_type_name = "dhis2-connection"

    base_url: str = Field(
        default="https://play.im.dhis2.org/dev",
        description="DHIS2 instance base URL",
    )
    username: str = Field(default="admin", description="DHIS2 username")
    password: SecretStr = Field(
        default=SecretStr("district"),
        description="DHIS2 password",
    )

    def get_client(self) -> httpx.Client:
        """Return an authenticated httpx client scoped to /api."""
        return httpx.Client(
            base_url=f"{self.base_url}/api",
            auth=(self.username, self.password.get_secret_value()),
            timeout=60,
        )

    def get_server_info(self) -> dict[str, Any]:
        """Fetch /api/system/info -- version, revision, etc."""
        with self.get_client() as client:
            resp = client.get("/system/info")
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
        with self.get_client() as client:
            resp = client.get(
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
        with self.get_client() as client:
            resp = client.get("/analytics", params=params)
            resp.raise_for_status()
            result: dict[str, Any] = resp.json()
            return result


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class Dhis2ApiResponse(BaseModel):
    """Wrapper for an API response summary."""

    endpoint: str
    record_count: int
    status_code: int = 200


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

OPERAND_PATTERN = re.compile(r"#\{[^}]+\}")


def get_dhis2_connection() -> Dhis2Connection:
    """Load the DHIS2 connection block, falling back to inline defaults.

    Returns:
        Dhis2Connection instance.
    """
    try:
        return Dhis2Connection.load("dhis2")  # type: ignore[return-value]
    except Exception:
        return Dhis2Connection()
