"""DHIS2 response models."""

from __future__ import annotations

from pydantic import BaseModel


class Dhis2ApiResponse(BaseModel):
    """Wrapper for an API response summary."""

    endpoint: str
    record_count: int
    status_code: int = 200
