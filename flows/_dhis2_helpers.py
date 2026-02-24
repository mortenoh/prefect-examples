"""Shared DHIS2 helpers -- blocks, models, and API client.

Provides the ``Dhis2Connection`` custom block, typed response models, and
the ``fetch_metadata`` function that calls the real DHIS2 API via httpx.
Used by flows 101--108.

The DHIS2 play server (https://play.dhis2.org/40) is publicly available
with credentials admin/district.
"""

from __future__ import annotations

import re
from typing import Any

import httpx
from prefect.blocks.core import Block
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Custom Block
# ---------------------------------------------------------------------------


class Dhis2Connection(Block):
    """Connection details for a DHIS2 instance.

    Equivalent to ``BaseHook.get_connection("dhis2_default")`` in Airflow.
    Register once via the Prefect UI or ``Dhis2Connection(...).save("dhis2")``.
    """

    _block_type_name = "dhis2-connection"

    base_url: str = "https://play.dhis2.org/40"
    username: str = "admin"
    api_version: str = "40"


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


def get_dhis2_connection() -> Dhis2Connection:
    """Load the DHIS2 connection block, falling back to inline defaults.

    Returns:
        Dhis2Connection instance.
    """
    try:
        return Dhis2Connection.load("dhis2")  # type: ignore[return-value]
    except Exception:
        return Dhis2Connection()


def get_dhis2_password() -> str:
    """Load the DHIS2 password from a Secret block with fallback.

    Returns:
        Password string.
    """
    try:
        from prefect.blocks.system import Secret

        secret = Secret.load("dhis2-password")  # type: ignore[union-attr]
        return secret.get()  # type: ignore[union-attr,no-any-return]
    except Exception:
        return "district"


# ---------------------------------------------------------------------------
# Real API fetch
# ---------------------------------------------------------------------------

OPERAND_PATTERN = re.compile(r"#\{[^}]+\}")


def fetch_metadata(
    connection: Dhis2Connection,
    endpoint: str,
    password: str,
    fields: str = ":owner",
) -> list[dict[str, Any]]:
    """Fetch all records from a DHIS2 metadata endpoint.

    Mirrors the Airflow helper ``fetch_metadata()`` from
    ``airflow_examples/dhis2.py``.

    Args:
        connection: DHIS2 connection block.
        endpoint: API endpoint name (e.g. "organisationUnits").
        password: DHIS2 password.
        fields: The fields parameter for the DHIS2 API.

    Returns:
        List of metadata records as dicts.
    """
    url = f"{connection.base_url}/api/{endpoint}"
    resp = httpx.get(
        url,
        auth=(connection.username, password),
        params={"paging": "false", "fields": fields},
        timeout=60,
    )
    resp.raise_for_status()
    data: dict[str, Any] = resp.json()
    key = endpoint.split("?")[0]
    result: list[dict[str, Any]] = data[key]
    return result


def fetch_analytics(
    connection: Dhis2Connection,
    password: str,
    dimension: list[str],
    filter_param: str | None = None,
) -> dict[str, Any]:
    """Fetch analytics data from the DHIS2 analytics API.

    Args:
        connection: DHIS2 connection block.
        password: DHIS2 password.
        dimension: List of dimension parameters (e.g. "dx:uid1;uid2").
        filter_param: Optional filter parameter (e.g. "pe:LAST_4_QUARTERS").

    Returns:
        Raw analytics response dict with "headers" and "rows".
    """
    url = f"{connection.base_url}/api/analytics"
    params: dict[str, Any] = {"dimension": dimension}
    if filter_param:
        params["filter"] = filter_param
    resp = httpx.get(
        url,
        auth=(connection.username, password),
        params=params,
        timeout=60,
    )
    resp.raise_for_status()
    result: dict[str, Any] = resp.json()
    return result
