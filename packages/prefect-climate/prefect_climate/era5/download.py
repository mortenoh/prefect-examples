"""ERA5-Land monthly-mean temperature download via earthkit-data.

Uses the CDS (Climate Data Store) API through ``earthkit.data`` to fetch
ERA5-Land monthly averaged 2m temperature. Results are converted from
Kelvin to Celsius and saved as GeoTIFF files.

CDS credentials are read from environment variables by earthkit-data:
- ``CDSAPI_URL`` (default: ``https://cds.climate.copernicus.eu/api``)
- ``CDSAPI_KEY`` (user's CDS API key)
"""

from __future__ import annotations

import logging
from pathlib import Path

import earthkit.data
import numpy as np
import rioxarray  # noqa: F401 -- registers .rio accessor
import xarray as xr

logger = logging.getLogger(__name__)

KELVIN_OFFSET = 273.15


def fetch_era5_monthly(
    variable: str,
    year: int,
    months: list[int],
    area: list[float],
    cache_dir: Path,
) -> dict[int, Path]:
    """Download ERA5-Land monthly-mean data and save as GeoTIFF per month.

    Args:
        variable: ERA5 variable name (e.g. ``"2m_temperature"``).
        year: Data year.
        months: List of month numbers (1-12).
        area: Bounding box as ``[north, west, south, east]``.
        cache_dir: Local directory for cached downloads.

    Returns:
        Dict mapping month number to local GeoTIFF path.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    result: dict[int, Path] = {}

    # Check cache first -- skip CDS request if all months are cached
    all_cached = True
    for month in months:
        dest = cache_dir / f"era5_{variable}_{year}_{month:02d}.tif"
        if dest.exists():
            result[month] = dest
        else:
            all_cached = False

    if all_cached:
        logger.info("All %d months cached for %s/%d", len(months), variable, year)
        return result

    logger.info("Fetching ERA5-Land %s for %d months=%s area=%s", variable, year, months, area)

    ds = earthkit.data.from_source(
        "cds",
        "reanalysis-era5-land-monthly-means",
        variable=variable,
        product_type="monthly_averaged_reanalysis",
        year=str(year),
        month=[f"{m:02d}" for m in months],
        time="00:00",
        area=area,
    )

    xds: xr.Dataset = ds.to_xarray()

    # The variable name in the dataset may differ from the request name.
    # For 2m_temperature, the NetCDF variable is typically "t2m".
    data_vars = list(xds.data_vars)
    if len(data_vars) != 1:
        logger.warning("Expected 1 data variable, got %d: %s", len(data_vars), data_vars)
    var_name = data_vars[0]

    for month in months:
        dest = cache_dir / f"era5_{variable}_{year}_{month:02d}.tif"
        if dest.exists():
            result[month] = dest
            continue

        # Select the time step for this month
        time_sel = f"{year}-{month:02d}"
        da: xr.DataArray = xds[var_name].sel(time=time_sel)

        # Convert Kelvin to Celsius for temperature variables
        if "temperature" in variable.lower() or var_name in ("t2m", "t2m_mean"):
            da = da - KELVIN_OFFSET
            logger.info("Converted %s month %02d from K to C", var_name, month)

        # Ensure CRS is set (ERA5 uses EPSG:4326)
        if da.rio.crs is None:
            da = da.rio.write_crs("EPSG:4326")

        # Drop time dimension for single-band GeoTIFF
        if "time" in da.dims:
            da = da.squeeze("time", drop=True)

        # Ensure spatial dims are named correctly for rioxarray
        spatial_dims = {"latitude": "y", "longitude": "x"}
        rename_map = {k: v for k, v in spatial_dims.items() if k in da.dims}
        if rename_map:
            da = da.rename(rename_map)

        da = da.astype(np.float32)
        da.rio.to_raster(str(dest))
        logger.info("Saved ERA5 %s %d-%02d -> %s", variable, year, month, dest.name)
        result[month] = dest

    return result
