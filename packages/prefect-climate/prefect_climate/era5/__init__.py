"""ERA5-Land monthly data download via earthkit-data."""

import math

from prefect_climate.era5.download import fetch_era5_monthly


def relative_humidity(temperature_c: float, dewpoint_c: float) -> float:
    """Derive relative humidity (%) from temperature and dewpoint in Celsius.

    Uses the Magnus formula approximation.

    Args:
        temperature_c: Air temperature in degrees Celsius.
        dewpoint_c: Dewpoint temperature in degrees Celsius.

    Returns:
        Relative humidity as a percentage (0-100).
    """
    a, b = 17.625, 243.04
    rh = 100.0 * math.exp(a * dewpoint_c / (b + dewpoint_c)) / math.exp(a * temperature_c / (b + temperature_c))
    return min(rh, 100.0)


__all__ = [
    "fetch_era5_monthly",
    "relative_humidity",
]
