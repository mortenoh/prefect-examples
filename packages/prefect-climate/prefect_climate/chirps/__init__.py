"""CHIRPS v3.0 daily rainfall download and monthly aggregation."""

from prefect_climate.chirps.download import (
    CHIRPS_BASE_URL,
    DEFAULT_AGGREGATION,
    DEFAULT_FLAVOR,
    DEFAULT_STAGE,
    Aggregation,
    build_chirps_day_url,
    fetch_chirps_monthly,
)

__all__ = [
    "CHIRPS_BASE_URL",
    "DEFAULT_AGGREGATION",
    "DEFAULT_FLAVOR",
    "DEFAULT_STAGE",
    "Aggregation",
    "build_chirps_day_url",
    "fetch_chirps_monthly",
]
