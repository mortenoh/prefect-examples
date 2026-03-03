"""ERA5-Land monthly data download and derived-variable helpers.

This module provides:

- ``fetch_era5_monthly`` -- download ERA5-Land monthly-mean data from CDS
- ``relative_humidity`` -- derive RH (%) from temperature and dewpoint
- ``wind_speed`` -- derive wind speed (m/s) from u and v components

Derived variables are computed *after* download because they combine two
separate CDS variables that must be fetched independently.
"""

import math

from prefect_climate.era5.download import fetch_era5_monthly


def relative_humidity(temperature_c: float, dewpoint_c: float) -> float:
    """Derive relative humidity (%) from temperature and dewpoint in Celsius.

    Uses the Magnus formula approximation::

        RH = 100 * exp(a * Td / (b + Td)) / exp(a * T / (b + T))

    where a = 17.625 and b = 243.04 (August-Roche-Magnus constants).

    Args:
        temperature_c: Air temperature in degrees Celsius (from ``2m_temperature``).
        dewpoint_c: Dewpoint temperature in degrees Celsius (from ``2m_dewpoint_temperature``).

    Returns:
        Relative humidity as a percentage (0--100).  Capped at 100 to guard
        against floating-point overshoot when T is very close to Td.
    """
    a, b = 17.625, 243.04
    rh = 100.0 * math.exp(a * dewpoint_c / (b + dewpoint_c)) / math.exp(a * temperature_c / (b + temperature_c))
    return min(rh, 100.0)


def wind_speed(u: float, v: float) -> float:
    """Compute wind speed (m/s) from u and v wind components.

    ERA5-Land provides wind as two orthogonal components at 10 m height:

    - **u10** (``10m_u_component_of_wind``): eastward (zonal) component in m/s
    - **v10** (``10m_v_component_of_wind``): northward (meridional) component in m/s

    The scalar wind speed is the vector magnitude::

        WS = sqrt(u^2 + v^2)

    This is the standard scalar wind speed used in epidemiological models
    for disease-vector dispersal (e.g. malaria, dengue).

    Args:
        u: Eastward wind component in m/s (zonal mean of ``u10`` raster).
        v: Northward wind component in m/s (zonal mean of ``v10`` raster).

    Returns:
        Wind speed in m/s.
    """
    return math.sqrt(u * u + v * v)


__all__ = [
    "fetch_era5_monthly",
    "relative_humidity",
    "wind_speed",
]
