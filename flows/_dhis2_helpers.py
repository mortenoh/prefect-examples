"""Shared DHIS2 helpers -- blocks, models, and simulated API client.

Provides the ``Dhis2Connection`` custom block, typed response models, and
deterministic data generators used by flows 101--108.  The simulated fetch
function mirrors real ``httpx`` usage so that swapping to a live DHIS2
instance requires changing a single function.
"""

from __future__ import annotations

import re

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
    """Wrapper for a simulated API response."""

    endpoint: str
    record_count: int
    status_code: int = 200


class RawOrgUnit(BaseModel):
    """Raw org unit matching the DHIS2 ``/api/organisationUnits`` shape."""

    id: str
    name: str
    shortName: str
    level: int
    parent: dict[str, str] | None = None
    path: str
    createdBy: dict[str, str] | None = None
    translations: list[dict[str, str]] = []
    openingDate: str = "2020-01-01"
    geometry: dict[str, object] | None = None


class RawDataElement(BaseModel):
    """Raw data element matching ``/api/dataElements``."""

    id: str
    name: str
    shortName: str
    domainType: str
    valueType: str
    aggregationType: str
    categoryCombo: dict[str, str] | None = None
    code: str | None = None


class RawIndicator(BaseModel):
    """Raw indicator matching ``/api/indicators``."""

    id: str
    name: str
    shortName: str
    indicatorType: dict[str, str] | None = None
    numerator: str
    denominator: str


class RawAnalyticsResponse(BaseModel):
    """Raw analytics response matching ``/api/analytics``."""

    headers: list[dict[str, str]]
    rows: list[list[str]]


# ---------------------------------------------------------------------------
# Deterministic data generators
# ---------------------------------------------------------------------------

_PARENT_MAP: dict[int, tuple[str, str]] = {
    1: ("", ""),
    2: ("OU_001", "National"),
    3: ("OU_002", "Region North"),
    4: ("OU_004", "District Alpha"),
}

_GEOMETRY_TYPES = ["Point", "Polygon", "Point", "Polygon"]
_GEOMETRY_COORDS: dict[str, list[float] | list[list[list[float]]]] = {
    "Point": [32.5, -1.5],
    "Polygon": [[[32.0, -2.0], [33.0, -2.0], [33.0, -1.0], [32.0, -1.0], [32.0, -2.0]]],
}


def simulate_org_units(n: int = 20) -> list[RawOrgUnit]:
    """Generate *n* deterministic org units across 4 hierarchy levels.

    Args:
        n: Number of org units to generate.

    Returns:
        List of RawOrgUnit.
    """
    units: list[RawOrgUnit] = []
    for i in range(1, n + 1):
        level = ((i - 1) % 4) + 1
        parent_id, parent_name = _PARENT_MAP[level]
        parent = {"id": parent_id, "name": parent_name} if parent_id else None
        path_parts = [f"OU_{j:03d}" for j in range(1, level)] + [f"OU_{i:03d}"]
        path = "/" + "/".join(path_parts)
        translations = [{"locale": "fr", "value": f"Unite_{i}"}] if i % 3 == 0 else []
        geom_type = _GEOMETRY_TYPES[(i - 1) % len(_GEOMETRY_TYPES)]
        lon_offset = i * 0.1
        lat_offset = i * 0.05
        if geom_type == "Point":
            geometry: dict[str, object] = {"type": "Point", "coordinates": [32.0 + lon_offset, -1.0 + lat_offset]}
        else:
            geometry = {
                "type": "Polygon",
                "coordinates": [
                    [
                        [32.0 + lon_offset, -2.0 + lat_offset],
                        [33.0 + lon_offset, -2.0 + lat_offset],
                        [33.0 + lon_offset, -1.0 + lat_offset],
                        [32.0 + lon_offset, -1.0 + lat_offset],
                        [32.0 + lon_offset, -2.0 + lat_offset],
                    ]
                ],
            }
        units.append(
            RawOrgUnit(
                id=f"OU_{i:03d}",
                name=f"OrgUnit_{i}",
                shortName=f"OU{i}",
                level=level,
                parent=parent,
                path=path,
                createdBy={"username": "admin"} if i % 2 == 0 else None,
                translations=translations,
                openingDate=f"2020-{((i - 1) % 12) + 1:02d}-01",
                geometry=geometry,
            )
        )
    return units


_VALUE_TYPES = ["NUMBER", "TEXT", "BOOLEAN", "INTEGER", "DATE"]
_AGGREGATION_TYPES = ["SUM", "AVERAGE", "COUNT", "NONE", "LAST"]
_DOMAIN_TYPES = ["AGGREGATE", "TRACKER"]


def simulate_data_elements(n: int = 15) -> list[RawDataElement]:
    """Generate *n* deterministic data elements.

    Args:
        n: Number of data elements.

    Returns:
        List of RawDataElement.
    """
    elements: list[RawDataElement] = []
    for i in range(1, n + 1):
        cc = {"id": f"CC_{(i % 3) + 1:03d}", "name": f"CategoryCombo_{(i % 3) + 1}"} if i % 4 != 0 else None
        code = f"DE_CODE_{i}" if i % 3 != 0 else None
        elements.append(
            RawDataElement(
                id=f"DE_{i:03d}",
                name=f"DataElement_{i}",
                shortName=f"DE{i}",
                domainType=_DOMAIN_TYPES[i % len(_DOMAIN_TYPES)],
                valueType=_VALUE_TYPES[i % len(_VALUE_TYPES)],
                aggregationType=_AGGREGATION_TYPES[i % len(_AGGREGATION_TYPES)],
                categoryCombo=cc,
                code=code,
            )
        )
    return elements


def simulate_indicators(n: int = 10) -> list[RawIndicator]:
    """Generate *n* deterministic indicators with expressions.

    Numerator and denominator expressions use ``#{uid.uid}`` operands and
    arithmetic operators, matching the real DHIS2 format.

    Args:
        n: Number of indicators.

    Returns:
        List of RawIndicator.
    """
    indicators: list[RawIndicator] = []
    for i in range(1, n + 1):
        # Build expressions of increasing complexity
        operand_count = ((i - 1) % 4) + 1
        operands = [f"#{{abc{j:02d}.def{j:02d}}}" for j in range(1, operand_count + 1)]
        operators = ["+", "-", "*", "/"]
        numerator_parts: list[str] = []
        for j, op in enumerate(operands):
            numerator_parts.append(op)
            if j < len(operands) - 1:
                numerator_parts.append(operators[j % len(operators)])
        numerator = "".join(numerator_parts)

        denom_count = max(1, operand_count - 1)
        denom_operands = [f"#{{xyz{j:02d}.uvw{j:02d}}}" for j in range(1, denom_count + 1)]
        denominator_parts: list[str] = []
        for j, op in enumerate(denom_operands):
            denominator_parts.append(op)
            if j < len(denom_operands) - 1:
                denominator_parts.append(operators[j % len(operators)])
        denominator = "".join(denominator_parts)

        indicators.append(
            RawIndicator(
                id=f"IND_{i:03d}",
                name=f"Indicator_{i}",
                shortName=f"IND{i}",
                indicatorType={"id": f"IT_{(i % 3) + 1:03d}", "name": f"Type_{(i % 3) + 1}"},
                numerator=numerator,
                denominator=denominator,
            )
        )
    return indicators


def simulate_analytics(
    data_elements: list[str],
    org_units: list[str],
    periods: list[str],
) -> RawAnalyticsResponse:
    """Generate a deterministic analytics response.

    Produces one row per (data_element, org_unit, period) combination.

    Args:
        data_elements: Data element UIDs (dx dimension).
        org_units: Org unit UIDs (ou dimension).
        periods: Period identifiers (pe dimension).

    Returns:
        RawAnalyticsResponse with headers and rows.
    """
    headers = [
        {"name": "dx", "column": "Data"},
        {"name": "ou", "column": "Organisation unit"},
        {"name": "pe", "column": "Period"},
        {"name": "value", "column": "Value"},
    ]
    rows: list[list[str]] = []
    counter = 100
    for dx in data_elements:
        for ou in org_units:
            for pe in periods:
                rows.append([dx, ou, pe, str(counter)])
                counter += 7
    return RawAnalyticsResponse(headers=headers, rows=rows)


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
    except (ValueError, Exception):
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
# Simulated API fetch
# ---------------------------------------------------------------------------

OPERAND_PATTERN = re.compile(r"#\{[^}]+\}")


def dhis2_api_fetch(
    connection: Dhis2Connection,
    endpoint: str,
    password: str,
    fields: list[str] | None = None,
) -> list[dict[str, object]]:
    """Simulated DHIS2 API fetch.

    In a real implementation, replace this function body with::

        url = f"{connection.base_url}/api/{connection.api_version}/{endpoint}"
        response = httpx.get(url, auth=(connection.username, password),
                             params={"fields": ",".join(fields)} if fields else {})
        response.raise_for_status()
        return response.json().get(endpoint, response.json())

    Args:
        connection: DHIS2 connection block.
        endpoint: API endpoint name (e.g. "organisationUnits").
        password: DHIS2 password.
        fields: Optional list of field names to request.

    Returns:
        List of dicts representing API records.
    """
    _ = connection, password, fields  # consumed by real implementation
    if endpoint == "organisationUnits":
        return [u.model_dump() for u in simulate_org_units()]
    elif endpoint == "dataElements":
        return [e.model_dump() for e in simulate_data_elements()]
    elif endpoint == "indicators":
        return [ind.model_dump() for ind in simulate_indicators()]
    else:
        return []
