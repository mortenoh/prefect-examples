"""ERA5-Land monthly-mean data download via earthkit-data.

Uses the CDS (Climate Data Store) API through ``earthkit.data`` to fetch
ERA5-Land monthly averaged variables. Unit conversions applied at download:

- Temperature (``t2m``, ``d2m``, ``skt``): Kelvin to Celsius
- Precipitation (``tp``): mean daily rate (m/day) to monthly total (mm)
- Solar radiation (``ssrd``): J/m2/day to W/m2 (mean daily irradiance)

Wind components (``u10``, ``v10``) and soil moisture (``swvl1``) are kept in
their native units (m/s and m3/m3 respectively).

CDS credentials are read from environment variables by earthkit-data:
- ``CDSAPI_URL`` (default: ``https://cds.climate.copernicus.eu/api``)
- ``CDSAPI_KEY`` (user's CDS API key)
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

# earthkit-data requires both CDSAPI_URL and CDSAPI_KEY in the environment.
# Set the URL default before importing earthkit so its prompt system finds it.
os.environ.setdefault("CDSAPI_URL", "https://cds.climate.copernicus.eu/api")

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

    # CDS may return "time" or "forecast_reference_time" as the temporal dim.
    time_dim = "time"
    for candidate in ("time", "forecast_reference_time", "valid_time"):
        if candidate in xds.dims:
            time_dim = candidate
            break
    logger.info("Using temporal dimension: %s", time_dim)

    for month in months:
        dest = cache_dir / f"era5_{variable}_{year}_{month:02d}.tif"
        if dest.exists():
            result[month] = dest
            continue

        # Select the time step for this month
        time_sel = f"{year}-{month:02d}"
        da: xr.DataArray = xds[var_name].sel({time_dim: time_sel})

        # -- Temperature conversion: Kelvin -> Celsius -----------------------
        # Applies to: 2m_temperature (t2m), 2m_dewpoint_temperature (d2m),
        # and skin_temperature (skt).  CDS delivers all temperature variables
        # in Kelvin; subtract 273.15 to get degrees Celsius.
        if "temperature" in variable.lower() or var_name in ("t2m", "t2m_mean", "d2m", "skt"):
            da = da - KELVIN_OFFSET
            logger.info("Converted %s month %02d from K to C", var_name, month)

        # -- Solar radiation conversion: J/m2/day -> W/m2 -------------------
        # Applies to: surface_solar_radiation_downwards (ssrd).
        # CDS monthly means give the mean daily energy flux in J/m2.
        # Dividing by 86 400 (seconds per day) yields mean irradiance in W/m2
        # (watts = joules per second).  This is the standard unit used by
        # climate/health applications for UV exposure and energy balance.
        if "radiation" in variable.lower() or var_name in ("ssrd",):
            da = da / 86400
            logger.info("Converted %s month %02d from J/m2 to W/m2", var_name, month)

        # -- Precipitation conversion: m/day -> mm/month --------------------
        # Applies to: total_precipitation (tp).
        # ERA5-Land monthly means give the average daily total in metres.
        # Multiply by the number of days in the month to get monthly total,
        # then by 1000 to convert metres to millimetres.
        if "precipitation" in variable.lower() or var_name in ("tp",):
            import calendar

            days_in_month = calendar.monthrange(year, month)[1]
            da = da * days_in_month * 1000
            logger.info("Converted %s month %02d from m/day to mm (%d days)", var_name, month, days_in_month)

        # -- No conversion needed -------------------------------------------
        # Wind components (u10, v10): already in m/s.
        # Soil moisture (swvl1): already in m3/m3 (volumetric fraction).

        # Ensure CRS is set (ERA5 uses EPSG:4326)
        if da.rio.crs is None:
            da = da.rio.write_crs("EPSG:4326")

        # Drop time dimension for single-band GeoTIFF
        if time_dim in da.dims:
            da = da.squeeze(time_dim, drop=True)

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
