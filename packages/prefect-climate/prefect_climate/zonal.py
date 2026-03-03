"""Zonal statistics for raster data.

Provides ``zonal_sum`` (for population rasters) and ``zonal_mean`` (for
climate rasters), plus a ``bounding_box`` helper for computing the extent
of a set of org unit geometries.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import rioxarray
import xarray as xr
from prefect_dhis2 import OrgUnitGeo


def zonal_sum(tiff_path: Path, geometry: dict[str, Any]) -> float:
    """Sum raster pixel values within a polygon boundary.

    Opens the raster with ``rioxarray``, clips to the given GeoJSON geometry,
    and sums all valid pixel values (excluding nodata).

    Args:
        tiff_path: Path to a GeoTIFF file.
        geometry: GeoJSON geometry dict (Polygon or MultiPolygon).

    Returns:
        Total (sum of pixel values) inside the geometry.
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


def zonal_mean(tiff_path: Path, geometry: dict[str, Any]) -> float:
    """Mean raster pixel value within a polygon boundary.

    Opens the raster with ``rioxarray``, clips to the given GeoJSON geometry,
    and returns the mean of all valid pixel values (excluding nodata).

    Args:
        tiff_path: Path to a GeoTIFF file.
        geometry: GeoJSON geometry dict (Polygon or MultiPolygon).

    Returns:
        Mean pixel value inside the geometry.
    """
    da: xr.DataArray = rioxarray.open_rasterio(tiff_path)  # type: ignore[assignment]
    try:
        clipped: xr.DataArray = da.rio.clip([geometry], all_touched=True)
        nodata = clipped.rio.nodata
        if nodata is not None:
            clipped = clipped.where(clipped != nodata)
        mean: float = float(np.nanmean(clipped.values))
    finally:
        da.close()
    return mean


def centroid(geometry: dict[str, Any]) -> tuple[float, float]:
    """Compute centroid (lat, lon) from a GeoJSON geometry.

    Extracts all coordinates from a Polygon or MultiPolygon geometry
    and returns the mean latitude and longitude.

    Args:
        geometry: GeoJSON geometry dict (Polygon or MultiPolygon).

    Returns:
        Tuple of (latitude, longitude) in decimal degrees.
    """
    lons: list[float] = []
    lats: list[float] = []
    _extract_coords(geometry.get("coordinates", []), lons, lats)

    if not lons or not lats:
        msg = "No coordinates found in geometry"
        raise ValueError(msg)

    return (sum(lats) / len(lats), sum(lons) / len(lons))


def bounding_box(org_units: list[OrgUnitGeo]) -> list[float]:
    """Compute [N, W, S, E] bounding box from org unit geometries.

    Iterates over all coordinates in every org unit geometry and returns
    the bounding extent. Used by ERA5 to limit the download area.

    Args:
        org_units: Org units with GeoJSON geometry.

    Returns:
        List of [north, west, south, east] in decimal degrees.
    """
    all_lons: list[float] = []
    all_lats: list[float] = []

    for ou in org_units:
        coords = ou.geometry.get("coordinates", [])
        _extract_coords(coords, all_lons, all_lats)

    if not all_lons or not all_lats:
        msg = "No coordinates found in org unit geometries"
        raise ValueError(msg)

    north = max(all_lats)
    south = min(all_lats)
    east = max(all_lons)
    west = min(all_lons)

    # Add a small buffer (~10 km) to avoid edge clipping
    buffer = 0.1
    return [north + buffer, west - buffer, south - buffer, east + buffer]


def _extract_coords(
    coords: Any,
    lons: list[float],
    lats: list[float],
) -> None:
    """Recursively extract lon/lat from nested coordinate arrays."""
    if not coords:
        return
    if isinstance(coords[0], (int, float)):
        lons.append(float(coords[0]))
        lats.append(float(coords[1]))
    else:
        for item in coords:
            _extract_coords(item, lons, lats)
