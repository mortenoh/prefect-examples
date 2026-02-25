"""DHIS2 utility helpers."""

from __future__ import annotations

import re

from prefect_dhis2.credentials import Dhis2Credentials

OPERAND_PATTERN = re.compile(r"#\{[^}]+\}")


def get_dhis2_credentials(name: str = "dhis2") -> Dhis2Credentials:
    """Load a DHIS2 credentials block, falling back to inline defaults.

    Attempts to load a saved block with the given *name* from the Prefect
    server.  If no saved block is found (or the server is unreachable),
    returns a fresh ``Dhis2Credentials`` instance with default play-server
    values.

    Args:
        name: Block name to load (default ``"dhis2"``).

    Returns:
        Dhis2Credentials instance.
    """
    try:
        return Dhis2Credentials.load(name)  # type: ignore[return-value]
    except Exception:
        return Dhis2Credentials()
