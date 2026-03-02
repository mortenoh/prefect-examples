"""WorldPop GeoTIFF download and zonal statistics.

Reusable module with no Prefect dependency. Downloads R2025A age/sex summary
rasters (T_M / T_F) from WorldPop and computes zonal population totals by
clipping rasters to GeoJSON polygon boundaries.

URL pattern::

    https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/
        R2025A/{year}/{ISO3}/v1/100m/constrained/
        {iso3}_T_{sex}_{year}_CN_100m_R2025A_v1.tif
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import httpx
import numpy as np
import rioxarray
import xarray as xr

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/R2025A"
RELEASE = "R2025A"
VERSION = "v1"


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------


def build_tiff_url(iso3: str, sex: str, year: int) -> str:
    """Construct the URL for a WorldPop total-sex summary raster.

    Args:
        iso3: ISO 3166-1 alpha-3 country code (e.g. "LAO").
        sex: ``"M"`` for male or ``"F"`` for female.
        year: Population year (2015-2030).

    Returns:
        Full URL to the GeoTIFF file.
    """
    iso_upper = iso3.upper()
    iso_lower = iso3.lower()
    return (
        f"{BASE_URL}/{year}/{iso_upper}/{VERSION}/100m/constrained/"
        f"{iso_lower}_T_{sex}_{year}_CN_100m_{RELEASE}_{VERSION}.tif"
    )


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------


def download_tiff(url: str, cache_dir: Path) -> Path:
    """Download a GeoTIFF to *cache_dir* if not already present.

    Uses httpx streaming to avoid loading the entire file into memory.

    Args:
        url: Remote URL of the GeoTIFF.
        cache_dir: Local directory for cached downloads.

    Returns:
        Path to the local GeoTIFF file.
    """
    filename = url.rsplit("/", maxsplit=1)[-1]
    dest = cache_dir / filename
    if dest.exists():
        logger.info("Cache hit: %s", dest.name)
        return dest

    cache_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading %s", url)

    with httpx.stream("GET", url, follow_redirects=True, timeout=300) as resp:
        resp.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in resp.iter_bytes(chunk_size=1024 * 64):
                fh.write(chunk)

    logger.info("Saved %s (%.1f MB)", dest.name, dest.stat().st_size / 1e6)
    return dest


def download_sex_rasters(
    iso3: str,
    year: int,
    cache_dir: Path,
) -> tuple[Path, Path]:
    """Download both T_M and T_F rasters for a country/year.

    Args:
        iso3: ISO 3166-1 alpha-3 country code.
        year: Population year (2015-2030).
        cache_dir: Local directory for cached downloads.

    Returns:
        Tuple of ``(male_path, female_path)``.
    """
    male_url = build_tiff_url(iso3, "M", year)
    female_url = build_tiff_url(iso3, "F", year)
    male_path = download_tiff(male_url, cache_dir)
    female_path = download_tiff(female_url, cache_dir)
    return male_path, female_path


# ---------------------------------------------------------------------------
# Zonal statistics
# ---------------------------------------------------------------------------


def zonal_population(tiff_path: Path, geometry: dict[str, Any]) -> float:
    """Sum raster pixel values within a polygon boundary.

    Opens the raster with ``rioxarray``, clips to the given GeoJSON geometry,
    and sums all valid pixel values (excluding nodata).

    Args:
        tiff_path: Path to a GeoTIFF file.
        geometry: GeoJSON geometry dict (Polygon or MultiPolygon).

    Returns:
        Total population (sum of pixel values) inside the geometry.
    """
    da: xr.DataArray = rioxarray.open_rasterio(tiff_path)  # type: ignore[assignment]
    try:
        clipped: xr.DataArray = da.rio.clip([geometry], all_touched=True)
        nodata = clipped.rio.nodata
        if nodata is not None:
            clipped = clipped.where(clipped != nodata)
        total: float = float(np.nansum(clipped.values))
    finally:
        da.close()
    return total


def population_by_sex(
    male_tiff: Path,
    female_tiff: Path,
    geometry: dict[str, Any],
) -> tuple[float, float]:
    """Return ``(male_total, female_total)`` for a geometry.

    Convenience wrapper around :func:`zonal_population`.

    Args:
        male_tiff: Path to the T_M GeoTIFF.
        female_tiff: Path to the T_F GeoTIFF.
        geometry: GeoJSON geometry dict.

    Returns:
        Tuple of (male population, female population).
    """
    male = zonal_population(male_tiff, geometry)
    female = zonal_population(female_tiff, geometry)
    return male, female
