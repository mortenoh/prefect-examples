"""WorldPop-specific data models for population import flows."""

from __future__ import annotations

from pathlib import Path

from prefect_dhis2 import OrgUnitGeo
from pydantic import BaseModel, Field


class WorldPopResult(BaseModel):
    """Population result for a single org unit."""

    org_unit_id: str
    org_unit_name: str
    male: float
    female: float


class RasterPair(BaseModel):
    """Paths to a pair of downloaded GeoTIFF rasters (e.g. male/female)."""

    male: Path
    female: Path


class AgePopulationResult(BaseModel):
    """Population result for a single org unit with age/sex disaggregation."""

    org_unit_id: str
    org_unit_name: str
    values: dict[str, float]  # keys like "M_0", "F_25"


class ImportQuery(BaseModel):
    """Parameters for a WorldPop GeoTIFF population import."""

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
