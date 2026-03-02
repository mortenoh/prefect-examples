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
| Coverage | 50S-50N (quasi-global); Africa-specific products available |
| Period | 1981 -- near-present |
| Format | GeoTIFF (gzipped for monthly Africa) |
| CRS | EPSG:4326 (WGS 84) |

## URL pattern

The pipeline downloads Africa monthly GeoTIFFs from the CHIRPS v2.0 archive:

```
https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_monthly/tifs/
    chirps-v2.0.{year}.{month:02d}.tif.gz
```

Each file is approximately 50 MB compressed, ~150 MB decompressed.

## File format

- Gzipped GeoTIFF (`.tif.gz`)
- Values in millimetres (mm) of rainfall
- NoData value: -9999
- Single band per file (one month)

## Pipeline overview

1. Fetch org unit geometries from DHIS2
2. Download monthly Africa GeoTIFFs (one per month)
3. Decompress gzipped files
4. Compute zonal mean rainfall per org unit per month
5. Import monthly values into DHIS2 (period format: `YYYYMM`)

## CHIRPS versions

| Version | Resolution | Notes |
|---------|-----------|-------|
| CHIRPS v2.0 | 0.05 degree | Used in this pipeline; stable, widely validated |
| CHIRPS v3.0 | 0.05 degree | Latest version with improved station blending |

The DHIS2 Climate Tools ecosystem uses CHIRPS v3.0. This pipeline currently
uses v2.0 for Africa monthly data. Upgrading to v3.0 requires only changing
the base URL.

## References

- [CHIRPS data portal](https://data.chc.ucsb.edu/products/CHIRPS-2.0/)
- [CHIRPS documentation](https://www.chc.ucsb.edu/data/chirps)
- [Funk et al. (2015)](https://doi.org/10.1038/sdata.2015.66) -- original CHIRPS paper
- [DHIS2 Climate Tools](https://climate-tools.dhis2.org/)
