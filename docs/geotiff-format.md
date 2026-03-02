# GeoTIFF Format and WorldPop Population Rasters

## What is GeoTIFF?

GeoTIFF is a raster image format based on TIFF (Tagged Image File Format) with
embedded georeferencing metadata. Each pixel in the image maps to a specific
geographic location via:

- **Coordinate Reference System (CRS)** -- defines the projection (e.g. EPSG:4326
  for WGS 84 lat/lon).
- **Affine transform** -- maps pixel row/column indices to geographic
  coordinates (origin, pixel size, rotation).
- **NoData value** -- a sentinel value marking pixels with no valid data (e.g.
  ocean, uninhabited areas).

GeoTIFF files use the `.tif` extension and can be read by any GIS tool
(QGIS, ArcGIS, GDAL) as well as Python libraries like `rasterio` and
`rioxarray`.

## How WorldPop encodes population

WorldPop publishes gridded population estimates as GeoTIFF rasters at ~100 m
resolution. Each pixel value represents the **estimated number of people**
living in that grid cell. Key properties:

- **CRS:** EPSG:4326 (WGS 84 geographic coordinates)
- **Pixel size:** ~100 m (approximately 0.000833 degrees at the equator)
- **Pixel values:** Floating-point population count (can be fractional)
- **NoData:** Typically `-99999` or `nan` for ocean, lakes, and uninhabited areas

To get the total population of a region, you sum all valid pixel values that
fall within its boundary -- this operation is called **zonal statistics**.

## WorldPop Age/Sex Structures dataset: R2025A

The **R2025A** release provides age- and sex-disaggregated population estimates
for **231 countries** covering **2015--2030**.

### Coverage

- 231 countries and territories worldwide (full global coverage including Asia,
  unlike the `wpgpas` API dataset which is limited mostly to Africa)
- Years: 2015, 2016, ..., 2030
- Resolution: 100 m (constrained variant)

### File naming convention

Files are organized under a consistent URL hierarchy:

```
https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/
  R2025A/{year}/{ISO3}/v1/100m/constrained/
    {iso3}_{group}_{year}_CN_100m_R2025A_v1.tif
```

Where:
- `{year}` -- four-digit year (e.g. `2024`)
- `{ISO3}` -- uppercase ISO 3166-1 alpha-3 code (e.g. `LAO`)
- `{iso3}` -- lowercase ISO code in the filename (e.g. `lao`)
- `{group}` -- age/sex group identifier (see below)

### Available files per country/year

Each country/year directory contains **42 individual rasters** plus **2 summary
rasters**:

**Individual age-group files** (20 age groups x 2 sexes = 40 files):

| Label | Age range | Male file | Female file |
|-------|-----------|-----------|-------------|
| `0`   | 0--12 months | `{iso3}_M_0_{year}_...` | `{iso3}_F_0_{year}_...` |
| `1`   | 1--4 years | `{iso3}_M_1_{year}_...` | `{iso3}_F_1_{year}_...` |
| `5`   | 5--9 years | `{iso3}_M_5_{year}_...` | `{iso3}_F_5_{year}_...` |
| `10`  | 10--14 years | `{iso3}_M_10_{year}_...` | `{iso3}_F_10_{year}_...` |
| `15`  | 15--19 years | `{iso3}_M_15_{year}_...` | `{iso3}_F_15_{year}_...` |
| `20`  | 20--24 years | `{iso3}_M_20_{year}_...` | `{iso3}_F_20_{year}_...` |
| `25`  | 25--29 years | `{iso3}_M_25_{year}_...` | `{iso3}_F_25_{year}_...` |
| `30`  | 30--34 years | `{iso3}_M_30_{year}_...` | `{iso3}_F_30_{year}_...` |
| `35`  | 35--39 years | `{iso3}_M_35_{year}_...` | `{iso3}_F_35_{year}_...` |
| `40`  | 40--44 years | `{iso3}_M_40_{year}_...` | `{iso3}_F_40_{year}_...` |
| `45`  | 45--49 years | `{iso3}_M_45_{year}_...` | `{iso3}_F_45_{year}_...` |
| `50`  | 50--54 years | `{iso3}_M_50_{year}_...` | `{iso3}_F_50_{year}_...` |
| `55`  | 55--59 years | `{iso3}_M_55_{year}_...` | `{iso3}_F_55_{year}_...` |
| `60`  | 60--64 years | `{iso3}_M_60_{year}_...` | `{iso3}_F_60_{year}_...` |
| `65`  | 65--69 years | `{iso3}_M_65_{year}_...` | `{iso3}_F_65_{year}_...` |
| `70`  | 70--74 years | `{iso3}_M_70_{year}_...` | `{iso3}_F_70_{year}_...` |
| `75`  | 75--79 years | `{iso3}_M_75_{year}_...` | `{iso3}_F_75_{year}_...` |
| `80`  | 80--84 years | `{iso3}_M_80_{year}_...` | `{iso3}_F_80_{year}_...` |
| `85`  | 85--89 years | `{iso3}_M_85_{year}_...` | `{iso3}_F_85_{year}_...` |
| `90`  | 90+ years | `{iso3}_M_90_{year}_...` | `{iso3}_F_90_{year}_...` |

**Summary rasters** (2 files -- these are what our flow downloads):

| Label | Description | Filename |
|-------|-------------|----------|
| `T_M` | Total male (sum of all male age groups) | `{iso3}_T_M_{year}_CN_100m_R2025A_v1.tif` |
| `T_F` | Total female (sum of all female age groups) | `{iso3}_T_F_{year}_CN_100m_R2025A_v1.tif` |

### Example download URLs

```
# Total female, Laos, 2024
https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/R2025A/2024/LAO/v1/100m/constrained/lao_T_F_2024_CN_100m_R2025A_v1.tif

# Total male, Laos, 2024
https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/R2025A/2024/LAO/v1/100m/constrained/lao_T_M_2024_CN_100m_R2025A_v1.tif
```

Each file is approximately 15 MB.

## Zonal statistics

Zonal statistics is the process of summarizing raster pixel values within
vector polygon boundaries. For population estimation:

1. **Load the raster** -- open the GeoTIFF with `rioxarray.open_rasterio()`
2. **Clip to polygon** -- use `.rio.clip([geometry])` to extract only the
   pixels that fall within the org unit boundary
3. **Handle nodata** -- mask out nodata pixels (ocean, lakes, etc.)
4. **Sum values** -- the sum of remaining pixel values gives the total
   population inside that boundary

This is done independently for the T_M (male) and T_F (female) rasters to
produce sex-disaggregated population counts.

## Comparison: API approach vs GeoTIFF approach

| Aspect | API (wpgpas/wpgppop) | GeoTIFF (R2025A) |
|--------|---------------------|-------------------|
| **Coverage** | wpgpas: mostly Africa; wpgppop: broader but no sex split | 231 countries worldwide |
| **Sex disaggregation** | wpgpas: yes; wpgppop fallback: 50/50 split | Always available (T_M / T_F) |
| **Data freshness** | Varies by dataset | 2015--2030 (annually) |
| **Network calls** | One HTTP POST per org unit per year | Two file downloads per country/year (shared across org units) |
| **Latency** | Async polling, 10--600 s per request | Seconds per zonal extraction (after download) |
| **Offline capable** | No -- requires live API | Yes -- once files are cached locally |
| **Area limit** | 100,000 km^2 per request | No limit (entire country raster) |
| **Accuracy** | Summary statistics from server | Full pixel-level extraction |
