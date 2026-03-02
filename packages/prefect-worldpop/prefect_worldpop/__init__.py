"""prefect-worldpop -- WorldPop GeoTIFF population data for Prefect flows."""

from prefect_worldpop.geotiff import (
    AGE_GROUPS,
    BASE_URL,
    RELEASE,
    VERSION,
    build_age_tiff_url,
    build_tiff_url,
    download_age_rasters,
    download_sex_rasters,
    download_tiff,
    population_by_sex,
    zonal_population,
)
from prefect_worldpop.schemas import AgePopulationResult, ImportQuery, ImportResult, RasterPair, WorldPopResult

__all__ = [
    "AGE_GROUPS",
    "BASE_URL",
    "RELEASE",
    "VERSION",
    "AgePopulationResult",
    "ImportQuery",
    "ImportResult",
    "RasterPair",
    "WorldPopResult",
    "build_age_tiff_url",
    "build_tiff_url",
    "download_age_rasters",
    "download_sex_rasters",
    "download_tiff",
    "population_by_sex",
    "zonal_population",
]
