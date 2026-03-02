"""ERA5-Land monthly temperature download via earthkit-data."""

from prefect_climate.era5.download import fetch_era5_monthly

__all__ = [
    "fetch_era5_monthly",
]
