# ERA5-Land

ERA5-Land is a global land-surface reanalysis dataset produced by ECMWF
(European Centre for Medium-Range Weather Forecasts) as part of the
Copernicus Climate Change Service (C3S). It provides hourly and monthly
estimates of atmospheric, land-surface, and soil variables at approximately
9 km (0.1 degree) resolution, from 1950 to near-present.

## Key characteristics

| Property | Value |
|----------|-------|
| Provider | ECMWF / Copernicus C3S |
| Spatial resolution | ~9 km (0.1 degree) |
| Temporal resolution | Hourly, monthly means |
| Coverage | Global land areas |
| Period | 1950 -- near-present |
| CRS | EPSG:4326 (WGS 84) |

## Variables used

The flow imports **monthly averaged 2m temperature**
(`reanalysis-era5-land-monthly-means`, variable `2m_temperature`). The raw
data is in Kelvin; the pipeline converts to Celsius before import.

Other commonly used ERA5-Land variables include total precipitation,
soil moisture, and evaporation.

## CDS API setup

ERA5-Land data is accessed through the Climate Data Store (CDS) API.

1. Register at <https://cds.climate.copernicus.eu/>
2. Accept the ERA5-Land licence in your CDS profile
3. Copy your API key from <https://cds.climate.copernicus.eu/profile>
4. Add your API key to `.env`:

```bash
# .env
CDSAPI_KEY=<your-api-key>
```

`CDSAPI_URL` defaults to `https://cds.climate.copernicus.eu/api` and does
not need to be set. The `earthkit-data` library reads these automatically.
CDS access requires the `cdsapi` package, which is included via the
`earthkit-data[cds]` extra in `prefect-climate`.

## earthkit-data usage

The pipeline uses `earthkit.data.from_source("cds", ...)` to request data
from the CDS API. This handles authentication, request queuing, and
format conversion transparently.

```python
import earthkit.data

ds = earthkit.data.from_source(
    "cds",
    "reanalysis-era5-land-monthly-means",
    variable="2m_temperature",
    product_type="monthly_averaged_reanalysis",
    year="2024",
    month=["01", "02", "03"],
    time="00:00",
    area=[10, -14, 7, -10],  # [N, W, S, E]
)
xds = ds.to_xarray()
```

## Pipeline overview

1. Fetch org unit geometries from DHIS2
2. Compute bounding box from org unit extents
3. Download ERA5-Land monthly temperature via earthkit-data
4. Convert Kelvin to Celsius
5. Save each month as GeoTIFF
6. Compute zonal mean per org unit per month
7. Import monthly values into DHIS2 (period format: `YYYYMM`)

## References

- [ERA5-Land documentation](https://confluence.ecmwf.int/display/CKB/ERA5-Land)
- [CDS API documentation](https://cds.climate.copernicus.eu/how-to-api)
- [earthkit-data documentation](https://earthkit-data.readthedocs.io/)
- [DHIS2 Climate Tools](https://climate-tools.dhis2.org/)
