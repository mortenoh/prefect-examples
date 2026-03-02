"""prefect-worldpop -- WorldPop GeoTIFF population data for Prefect flows."""

from prefect_worldpop.geotiff import (
    BASE_URL,
    RELEASE,
    VERSION,
    build_tiff_url,
    download_sex_rasters,
    download_tiff,
    population_by_sex,
    zonal_population,
)
from prefect_worldpop.schemas import ImportQuery, ImportResult, RasterPair, WorldPopResult

__all__ = [
    "BASE_URL",
    "RELEASE",
    "VERSION",
    "ImportQuery",
    "ImportResult",
    "RasterPair",
    "WorldPopResult",
    "build_tiff_url",
    "download_sex_rasters",
    "download_tiff",
    "population_by_sex",
    "zonal_population",
]
