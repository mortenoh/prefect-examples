# ISO 3166-1 Alpha-3 Country Codes

## What is ISO 3166-1 alpha-3?

ISO 3166-1 alpha-3 is the international standard for three-letter country codes
published by the International Organization for Standardization (ISO). Each code
uniquely identifies a country or territory (e.g. `SLE` for Sierra Leone, `LAO`
for Laos).

## Why WorldPop requires ISO3

WorldPop publishes population rasters organized per-country. Unlike some spatial
data services, WorldPop files **cannot be queried by bounding box** -- you must
specify the country by its ISO3 code to locate the correct raster. This is also
confirmed by the DHIS2 climate/Earth-observation tools (dhis2eo).

The WorldPop URL pattern for GeoTIFF rasters is:

```
https://data.worldpop.org/GIS/AgeSex_structures/Global_2000_2020_1km_UNadj/
  constrained/{YEAR}/{ISO3_UPPER}/{iso3_lower}_{sex}_{age}_{year}.tif
```

- **Uppercase** in URL path segments: `/SLE/`
- **Lowercase** in filenames: `sle_f_0_2020.tif`

## Common codes used in this project

| Code  | Country        | Notes                      |
|-------|----------------|----------------------------|
| `SLE` | Sierra Leone   | Default for dev DHIS2      |
| `LAO` | Laos           | Previously used as default |
| `VNM` | Vietnam        |                            |

## Where to find ISO3 codes

- [ISO 3166-1 alpha-3 -- Wikipedia](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3)
- [ISO Online Browsing Platform](https://www.iso.org/obp/ui/#search)

## How ISO3 is used in the codebase

1. **`ImportQuery.iso3`** -- The `iso3` field on `ImportQuery`
   (`packages/prefect-climate/prefect_climate/schemas.py`) is the main entry
   point. It is passed through every task in the flow.

2. **`build_tiff_url()`** -- Constructs the download URL by inserting the ISO3
   code in both uppercase (path) and lowercase (filename) positions.

3. **Default value** -- When no `ImportQuery` is provided the flow defaults to
   `iso3="SLE"` (Sierra Leone), matching the dev/play DHIS2 instance.
