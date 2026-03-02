"""WorldPop GeoTIFF population data -- download and zonal statistics."""

from prefect_climate.worldpop.geotiff import (
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
from prefect_climate.worldpop.schemas import AgePopulationResult, RasterPair, WorldPopResult

__all__ = [
    "AGE_GROUPS",
    "BASE_URL",
    "RELEASE",
    "VERSION",
    "AgePopulationResult",
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
