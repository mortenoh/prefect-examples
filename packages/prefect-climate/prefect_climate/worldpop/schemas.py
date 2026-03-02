"""WorldPop-specific data models for population import flows."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel


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
