# CHIRPS

CHIRPS (Climate Hazards group InfraRed Precipitation with Station data)
is a quasi-global rainfall dataset produced by the Climate Hazards Center
at UC Santa Barbara. It combines satellite imagery with in-situ station
data to produce gridded rainfall estimates.

## Key characteristics

| Property | Value |
|----------|-------|
| Provider | Climate Hazards Center, UCSB |
| Spatial resolution | ~5 km (0.05 degree) |
| Temporal resolution | Daily, pentadal, monthly |
| Coverage | Global (50S-50N) |
| Period | 1981 -- near-present |
| Format | GeoTIFF |
| CRS | EPSG:4326 (WGS 84) |

## URL pattern

The pipeline downloads daily GeoTIFFs from the CHIRPS v3.0 archive and
aggregates them to monthly totals locally:

```
https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/final/{flavor}/{YYYY}/
    chirps-v3.0.{flavor}.{YYYY}.{MM}.{DD}.tif
```

Preliminary data uses a different path:

```
https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/prelim/sat/{YYYY}/
    chirps-v3.0.prelim.{YYYY}.{MM}.{DD}.tif
```

Default settings use `stage="final"` and `flavor="rnl"` (rain + land).

## File format

- GeoTIFF (`.tif`)
- Values in millimetres (mm) of rainfall per day
- NoData value: -9999
- Single band per file (one day)
- Monthly output is aggregated locally (sum of daily values)

## Pipeline overview

1. Fetch org unit geometries from DHIS2
2. Compute bounding box from org unit extents
3. Download daily CHIRPS v3.0 GeoTIFFs for each month
4. Clip to bounding box and aggregate daily to monthly total (mm)
5. Compute zonal mean precipitation per org unit per month
6. Import monthly values into DHIS2 (period format: `YYYYMM`)

This approach follows the same pattern as
[dhis2eo](https://github.com/dhis2/dhis2eo/) -- fetching daily GeoTIFFs
from CHC servers, clipping to a bounding box, and aggregating to monthly
totals.

## CHIRPS versions

| Version | Resolution | Notes |
|---------|-----------|-------|
| CHIRPS v2.0 | 0.05 degree | Stable, widely validated; Africa-specific products available |
| CHIRPS v3.0 | 0.05 degree | Used in this pipeline; improved station blending, global daily files |

## References

- [CHIRPS v3.0 data portal](https://data.chc.ucsb.edu/products/CHIRPS/v3.0/)
- [CHIRPS documentation](https://www.chc.ucsb.edu/data/chirps)
- [Funk et al. (2015)](https://doi.org/10.1038/sdata.2015.66) -- original CHIRPS paper
- [DHIS2 Climate Tools](https://climate-tools.dhis2.org/)
- [dhis2eo](https://github.com/dhis2/dhis2eo/) -- aligned CHIRPS v3.0 approach
