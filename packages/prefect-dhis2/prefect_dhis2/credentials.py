"""DHIS2 credentials block and API client."""

from __future__ import annotations

from typing import Any

import httpx
from prefect.blocks.core import Block
from pydantic import Field, SecretStr


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

    def post_data_values(self, payload: dict[str, Any]) -> dict[str, Any]:
        """POST data values to /dataValueSets.

        Args:
            payload: JSON body with ``dataValues`` list.

        Returns:
            Parsed DHIS2 import summary response.
        """
        resp = self._http.post("/dataValueSets", json=payload)
        resp.raise_for_status()
        result: dict[str, Any] = resp.json()
        return result

    def fetch_organisation_units_by_code(
        self,
        codes: list[str],
        fields: str = "id,code,name",
    ) -> list[dict[str, Any]]:
        """Fetch organisation units matching the given codes.

        Args:
            codes: List of organisation unit codes (e.g. ISO3 country codes).
            fields: DHIS2 fields parameter.

        Returns:
            List of organisation unit dicts.
        """
        code_list = ",".join(codes)
        resp = self._http.get(
            "/organisationUnits",
            params={
                "filter": f"code:in:[{code_list}]",
                "fields": fields,
                "paging": "false",
            },
        )
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        result: list[dict[str, Any]] = data["organisationUnits"]
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


class Dhis2Credentials(Block):
    """Credentials block for a DHIS2 instance.

    Stores connection details (URL, username, password) and returns an
    authenticated ``Dhis2Client`` via ``get_client()``.

    Equivalent to ``BaseHook.get_connection("dhis2_default")`` in Airflow.
    """

    _block_type_name = "dhis2-credentials"
    _block_type_slug = "dhis2-credentials"
    _logo_url = "https://dhis2.org/wp-content/uploads/dhis2-logo-rgb-positive.svg"  # type: ignore[assignment]
    _description = "Credentials block for connecting to a DHIS2 instance."
    _documentation_url = "https://github.com/morteoh/prefect-examples/tree/main/packages/prefect-dhis2/docs"  # type: ignore[assignment]

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
