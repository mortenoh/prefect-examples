"""CHIRPS v3.0 global daily rainfall download.

Fetches daily GeoTIFFs from the CHC data archive, clips to a bounding
box, aggregates to monthly values, and saves as GeoTIFF.

Uses the same approach as dhis2eo: read remote daily files via rioxarray,
clip to bbox, concatenate into monthly arrays.

URL patterns (CHIRPS v3.0 daily)::

    Final:  .../CHIRPS/v3.0/daily/final/{flavor}/{YYYY}/
            chirps-v3.0.{flavor}.{YYYY}.{MM}.{DD}.tif
    Prelim: .../CHIRPS/v3.0/daily/prelim/sat/{YYYY}/
            chirps-v3.0.prelim.{YYYY}.{MM}.{DD}.tif
"""

from __future__ import annotations

import calendar
import logging
from enum import StrEnum
from pathlib import Path

import numpy as np
import rioxarray
import xarray as xr

logger = logging.getLogger(__name__)

CHIRPS_BASE_URL = "https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/"
DEFAULT_STAGE = "final"
DEFAULT_FLAVOR = "rnl"


class Aggregation(StrEnum):
    """Temporal aggregation method for daily-to-monthly conversion."""

    SUM = "sum"
    MEAN = "mean"
    MIN = "min"
    MAX = "max"


DEFAULT_AGGREGATION = Aggregation.SUM


def build_chirps_day_url(
    year: int,
    month: int,
    day: int,
    stage: str = DEFAULT_STAGE,
    flavor: str = DEFAULT_FLAVOR,
) -> str:
    """Construct the URL for a single CHIRPS v3.0 daily GeoTIFF.

    Args:
        year: Data year.
        month: Month number (1-12).
        day: Day of month (1-31).
        stage: Release stage -- ``"final"`` or ``"prelim"``.
        flavor: Data flavor -- ``"rnl"`` (rain + land) or ``"sat"`` (satellite only).
            Ignored when *stage* is ``"prelim"`` (always ``"sat"``).

    Returns:
        Full URL to the daily GeoTIFF file.
    """
    if stage == "prelim":
        return f"{CHIRPS_BASE_URL}prelim/sat/{year}/chirps-v3.0.prelim.{year}.{month:02d}.{day:02d}.tif"
    return f"{CHIRPS_BASE_URL}{stage}/{flavor}/{year}/chirps-v3.0.{flavor}.{year}.{month:02d}.{day:02d}.tif"


def fetch_chirps_day(url: str, bbox: tuple[float, float, float, float]) -> xr.DataArray:
    """Open a remote CHIRPS daily GeoTIFF and clip to bounding box.

    Args:
        url: Remote URL of the daily GeoTIFF.
        bbox: Bounding box as ``(west, south, east, north)``.

    Returns:
        Clipped xarray DataArray.
    """
    da: xr.DataArray = rioxarray.open_rasterio(url)  # type: ignore[assignment]
    west, south, east, north = bbox
    clipped: xr.DataArray = da.rio.clip_box(minx=west, miny=south, maxx=east, maxy=north)
    return clipped


def _aggregate(stacked: xr.DataArray, method: Aggregation) -> xr.DataArray:
    """Apply temporal aggregation along the ``time`` dimension."""
    if method == Aggregation.SUM:
        return stacked.sum(dim="time", skipna=True)
    if method == Aggregation.MEAN:
        return stacked.mean(dim="time", skipna=True)
    if method == Aggregation.MIN:
        return stacked.min(dim="time", skipna=True)
    if method == Aggregation.MAX:
        return stacked.max(dim="time", skipna=True)
    msg = f"Unknown aggregation method: {method}"
    raise ValueError(msg)


def fetch_chirps_monthly(
    year: int,
    month: int,
    area: list[float],
    cache_dir: Path,
    stage: str = DEFAULT_STAGE,
    flavor: str = DEFAULT_FLAVOR,
    aggregation: Aggregation = DEFAULT_AGGREGATION,
) -> Path:
    """Download daily CHIRPS v3.0 GeoTIFFs for one month and aggregate.

    Loops over every day in the month, fetches each daily GeoTIFF from the
    CHC archive, clips to the bounding box, aggregates daily values into a
    single monthly raster, and saves as GeoTIFF.

    The default aggregation is ``sum`` (daily mm summed to monthly mm).
    Other methods (``mean``, ``min``, ``max``) are available for different
    analysis needs.

    If the output file already exists in *cache_dir*, returns immediately
    (cache hit).

    Args:
        year: Data year.
        month: Month number (1-12).
        area: Bounding box as ``[N, W, S, E]`` (same format as
            :func:`prefect_climate.zonal.bounding_box`).
        cache_dir: Local directory for cached downloads.
        stage: Release stage -- ``"final"`` or ``"prelim"``.
        flavor: Data flavor -- ``"rnl"`` or ``"sat"``.
        aggregation: Temporal aggregation method. Default ``sum``.

    Returns:
        Path to the monthly GeoTIFF file.
    """
    suffix = f".{aggregation.value}" if aggregation != Aggregation.SUM else ""
    out_name = f"chirps-v3.0.{year}.{month:02d}{suffix}.tif"
    dest = cache_dir / out_name

    if dest.exists():
        logger.info("Cache hit: %s", dest.name)
        return dest

    cache_dir.mkdir(parents=True, exist_ok=True)

    north, west, south, east = area
    bbox = (west, south, east, north)

    num_days = calendar.monthrange(year, month)[1]
    daily_arrays: list[xr.DataArray] = []

    for day in range(1, num_days + 1):
        url = build_chirps_day_url(year, month, day, stage=stage, flavor=flavor)
        logger.info("Fetching %s", url)
        da = fetch_chirps_day(url, bbox)
        daily_arrays.append(da)

    stacked = xr.concat(daily_arrays, dim="time")
    # Replace nodata with NaN before aggregating so missing days don't corrupt results
    nodata = stacked.rio.nodata
    if nodata is not None:
        stacked = stacked.where(stacked != nodata, other=np.nan)
    monthly_result = _aggregate(stacked, aggregation)

    # Restore CRS and spatial metadata from the first daily array
    monthly_result = monthly_result.rio.write_crs(daily_arrays[0].rio.crs)
    monthly_result = monthly_result.rio.write_nodata(np.nan)
    monthly_result.rio.to_raster(str(dest))

    logger.info("Saved %s (%.1f MB)", dest.name, dest.stat().st_size / 1e6)
    return dest
