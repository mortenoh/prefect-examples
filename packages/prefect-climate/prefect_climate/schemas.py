"""Shared data models for climate and population import flows."""

from __future__ import annotations

from prefect_dhis2 import OrgUnitGeo
from pydantic import BaseModel, Field


class ImportQuery(BaseModel):
    """Parameters for a raster-based population import."""

    iso3: str = Field(description="ISO 3166-1 alpha-3 country code (e.g. 'SLE')")
    org_unit_level: int = Field(default=2, description="DHIS2 org unit level to import at")
    years: list[int] = Field(default=list(range(2020, 2026)))


class ImportResult(BaseModel):
    """Summary of a DHIS2 data value import."""

    dhis2_url: str = Field(description="Target DHIS2 instance URL")
    org_units: list[OrgUnitGeo] = Field(description="Org units with geometry")
    imported: int = Field(default=0, description="Records imported")
    updated: int = Field(default=0, description="Records updated")
    ignored: int = Field(default=0, description="Records ignored")
    total: int = Field(default=0, description="Total records sent")
    markdown: str = Field(default="", description="Markdown summary")


class ClimateQuery(BaseModel):
    """Parameters for a climate data import (ERA5 / CHIRPS)."""

    iso3: str = Field(default="SLE", description="ISO 3166-1 alpha-3 country code")
    org_unit_level: int = Field(default=2, description="DHIS2 org unit level to import at")
    year: int = Field(default=2024, description="Data year")
    months: list[int] = Field(default=list(range(1, 13)), description="Months to import (1-12)")


class ClimateResult(BaseModel):
    """Climate data result for a single org unit."""

    org_unit_id: str
    org_unit_name: str
    monthly_values: dict[int, float]  # keys are month numbers (1-12)
