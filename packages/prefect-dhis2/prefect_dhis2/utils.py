"""DHIS2 utility helpers."""

from __future__ import annotations

import re

from prefect_dhis2.credentials import Dhis2Credentials

OPERAND_PATTERN = re.compile(r"#\{[^}]+\}")


def get_dhis2_credentials() -> Dhis2Credentials:
    """Load the DHIS2 credentials block, falling back to inline defaults.

    Attempts to load a saved block named ``"dhis2"`` from the Prefect server.
    If no saved block is found (or the server is unreachable), returns a fresh
    ``Dhis2Credentials`` instance with default play-server values.

    Returns:
        Dhis2Credentials instance.
    """
    try:
        return Dhis2Credentials.load("dhis2")  # type: ignore[return-value]
    except Exception:
        return Dhis2Credentials()
