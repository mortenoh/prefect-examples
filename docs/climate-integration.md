# Climate Data Integration

This document describes the architecture for integrating climate and
population raster data into DHIS2 using the `prefect-climate` package.

## Supported data sources

| Source | Package module | Resolution | Variables |
|--------|---------------|-----------|-----------|
| WorldPop | `prefect_climate.worldpop` | ~100 m | Population (sex, age) |
| ERA5-Land | `prefect_climate.era5` | ~9 km | Temperature, precipitation, soil moisture |
| CHIRPS | `prefect_climate.chirps` | ~5 km | Rainfall |

## Architecture

```
prefect-climate/
    prefect_climate/
        schemas.py        # Shared models: ImportQuery, ClimateQuery, etc.
        zonal.py          # Zonal stats: zonal_sum, zonal_mean, bounding_box
        worldpop/         # WorldPop GeoTIFF download + population stats
        era5/             # ERA5-Land via earthkit-data + CDS API
        chirps/           # CHIRPS v2.0 Africa monthly rainfall
```

### Zonal statistics

The `zonal.py` module provides two core operations:

- **`zonal_sum`** -- used for population rasters where pixel values are
  counts that should be summed within a boundary
- **`zonal_mean`** -- used for climate rasters (temperature, rainfall)
  where the spatial average is meaningful

Both functions use `rioxarray` to clip a GeoTIFF to a GeoJSON polygon
and compute the statistic while handling nodata values.

### Flow pattern

All import flows follow the same structure:

1. **`ensure_dhis2_metadata`** -- Create/update DHIS2 data elements,
   data sets, and (optionally) category combos. All metadata objects
   include a `sharing` property: data sets default to `rwr-----`
   (metadata read/write, data read-only) so imported data is publicly
   visible but cannot be modified without explicit permission; data
   elements default to `rw------` (metadata read/write only, as DHIS2
   does not support data-level sharing on data elements).
2. **Download rasters** -- Fetch data from the source (CDS API, HTTP, etc.)
3. **Compute zonal stats** -- One task per org unit
4. **Build data values** -- Convert results to DHIS2 DataValueSet format
5. **Import to DHIS2** -- POST data values and report summary

### Period handling

| Data type | Period format | Example |
|-----------|--------------|---------|
| Population (yearly) | `YYYY` | `2024` |
| Climate (monthly) | `YYYYMM` | `202401` |

## ENACTS

ENACTS (Enhancing National Climate Services) is a climate data initiative by
the International Research Institute for Climate and Society (IRI) at Columbia
University. It produces high-resolution, quality-controlled climate datasets
by blending:

- **Weather station observations** (national meteorological services)
- **Satellite estimates** (CHIRPS for rainfall, various products for temperature)
- **Reanalysis data** (ERA5 for temperature)

ENACTS datasets are typically produced per country in collaboration with
national meteorological agencies, providing better accuracy than global
products alone in data-sparse regions. The output is gridded data at
resolutions typically matching or exceeding the input satellite products
(~5 km for rainfall, ~9 km for temperature).

ENACTS products include:

| Product | Resolution | Period |
|---------|-----------|--------|
| Rainfall | ~5 km (dekadal, monthly) | 1981 -- present |
| Temperature (min/max/mean) | ~9 km (dekadal, monthly) | 1981 -- present |

ENACTS is available for 30+ African countries through the IRI Data Library.
Integration with this pipeline would follow the same pattern: download
rasters, compute zonal stats, import to DHIS2. The existing `zonal_mean`
function works directly with ENACTS GeoTIFFs.

## DHIS2 Climate Tools ecosystem

This pipeline aligns with the DHIS2 Climate Tools ecosystem:

| Tool | Role |
|------|------|
| `earthkit-data` | Download climate data from CDS |
| `earthkit-transform` | Aggregate to org unit boundaries |
| `dhis2eo` | Earth observation data operations |
| `xarray` / `rioxarray` | Multi-dimensional array processing |
| `geopandas` | Geospatial analysis |

The `prefect-climate` package replaces the need for `dhis2eo` by providing
the same download-aggregate-import workflow as Prefect tasks with built-in
orchestration, retries, and observability.

## References

- [DHIS2 Climate Tools](https://climate-tools.dhis2.org/)
- [ENACTS at IRI](https://iri.columbia.edu/our-expertise/climate/enacts/)
- [earthkit documentation](https://earthkit.readthedocs.io/)
