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

The climate flow imports eight variables from
`reanalysis-era5-land-monthly-means` and produces seven climate
indicators per org unit per month:

### Downloaded variables and unit conversions

| Variable | CDS variable | NetCDF col | Raw units | Target | Conversion |
|----------|-------------|-----------|-----------|--------|------------|
| Temperature | `2m_temperature` | `t2m` | Kelvin | Celsius | `value - 273.15` |
| Precipitation | `total_precipitation` | `tp` | m/day (mean daily rate) | mm/month | `value * days * 1000` |
| Dewpoint | `2m_dewpoint_temperature` | `d2m` | Kelvin | Celsius | `value - 273.15` |
| Wind U | `10m_u_component_of_wind` | `u10` | m/s | m/s | none |
| Wind V | `10m_v_component_of_wind` | `v10` | m/s | m/s | none |
| Skin temperature | `skin_temperature` | `skt` | Kelvin | Celsius | `value - 273.15` |
| Solar radiation | `surface_solar_radiation_downwards` | `ssrd` | J/m2/day | W/m2 | `value / 86400` |
| Soil moisture | `volumetric_soil_water_layer_1` | `swvl1` | m3/m3 | m3/m3 | none |

### Derived variables

**Relative humidity** is derived from temperature (T) and dewpoint (Td) in
Celsius using the Magnus formula:

```
RH = 100 * exp(17.625 * Td / (243.04 + Td)) / exp(17.625 * T / (243.04 + T))
```

**Wind speed** is derived from the eastward (u) and northward (v) wind
components at 10 m height:

```
WS = sqrt(u10^2 + v10^2)
```

**Solar radiation** is converted from daily energy (J/m2) to mean daily
irradiance (W/m2). Since 1 watt = 1 joule per second:

```
W/m2 = J/m2/day / 86400 s/day
```

### Available ERA5-Land variables

ERA5-Land provides many variables relevant to health and climate analysis.
Below are commonly used ones:

| CDS variable name | NetCDF column | Native units | Description |
|---|---|---|---|
| `2m_temperature` | `t2m` | Kelvin | Air temperature at 2 m height |
| `total_precipitation` | `tp` | metres (cumulative) | Total precipitation per hour |
| `2m_dewpoint_temperature` | `d2m` | Kelvin | Dewpoint temperature at 2 m |
| `surface_pressure` | `sp` | Pa | Pressure at the surface |
| `10m_u_component_of_wind` | `u10` | m/s | Eastward wind component at 10 m |
| `10m_v_component_of_wind` | `v10` | m/s | Northward wind component at 10 m |
| `volumetric_soil_water_layer_1` | `swvl1` | m3/m3 | Top-level soil moisture (0-7 cm) |
| `total_evaporation` | `e` | metres (cumulative) | Total evaporation per hour |
| `skin_temperature` | `skt` | Kelvin | Land surface temperature |
| `surface_solar_radiation_downwards` | `ssrd` | J/m2 (cumulative) | Incoming solar radiation |

Cumulative variables (precipitation, evaporation, radiation) store running
totals that reset at specific intervals. They must be de-accumulated
(differenced) before temporal aggregation.

The full variable catalogue is available at
<https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land>.

### CDS datasets

| Dataset ID | Description |
|---|---|
| `reanalysis-era5-land-monthly-means` | Monthly averaged values (used by the climate flow) |
| `reanalysis-era5-land` | Hourly values (higher temporal resolution, larger downloads) |

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

`CDSAPI_URL` defaults to `https://cds.climate.copernicus.eu/api` (set
automatically by the `prefect-climate` package) and does not need to be
configured. CDS access requires the `cdsapi` package, which is included
via the `earthkit-data[cds]` extra in `prefect-climate`.

## earthkit-data usage

The pipeline uses `earthkit.data.from_source("cds", ...)` to request data
from the CDS API. This handles authentication, request queuing, and
format conversion transparently.

```python
import earthkit.data

# Request each variable in a separate CDS call
variables = [
    "2m_temperature",
    "total_precipitation",
    "2m_dewpoint_temperature",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "skin_temperature",
    "surface_solar_radiation_downwards",
    "volumetric_soil_water_layer_1",
]
for variable in variables:
    ds = earthkit.data.from_source(
        "cds",
        "reanalysis-era5-land-monthly-means",
        variable=variable,
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
3. Download 8 ERA5-Land monthly variables (temp, precip, dewpoint, wind u/v, skin temp, solar rad, soil moisture)
4. Convert units (K to C, m to mm/month, J/m2 to W/m2)
5. Save each month as GeoTIFF (one per variable)
6. Compute zonal mean per org unit per month
7. Derive relative humidity (Magnus formula) and wind speed (vector magnitude)
8. Import 7 monthly indicators into DHIS2 (period format: `YYYYMM`)

### DHIS2 data model

```
Data Set: PR: ERA5: Climate (PfE5ClmSet1)
  |- Data Element: PR: ERA5: Mean Temperature    (PfE5TmpEst1)
  |- Data Element: PR: ERA5: Total Precipitation (PfE5PrcEst1)
  |- Data Element: PR: ERA5: Relative Humidity   (PfE5HumEst1)
  |- Data Element: PR: ERA5: Wind Speed          (PfE5WndEst1)
  |- Data Element: PR: ERA5: Skin Temperature    (PfE5SknEst1)
  |- Data Element: PR: ERA5: Solar Radiation     (PfE5RadEst1)
  |- Data Element: PR: ERA5: Soil Moisture       (PfE5SoiEst1)
```

### Health relevance

Each variable supports specific health surveillance use cases:

| Variable | Health relevance |
|----------|-----------------|
| **Temperature** | Heat-related illness, malaria transmission windows (optimal 20-30 C) |
| **Precipitation** | Waterborne disease risk, flooding, mosquito breeding habitat |
| **Relative humidity** | Pathogen survival and airborne transmission, respiratory illness |
| **Wind speed** | Disease vector dispersal (malaria, dengue mosquitoes), air quality |
| **Skin temperature** | Urban heat islands, heat stress assessment, land surface conditions |
| **Solar radiation** | UV exposure risk, vitamin D synthesis, evapotranspiration driver |
| **Soil moisture** | Waterborne disease risk, agricultural health, flood prediction |

## References

- [ERA5-Land documentation](https://confluence.ecmwf.int/display/CKB/ERA5-Land)
- [CDS API documentation](https://cds.climate.copernicus.eu/how-to-api)
- [earthkit-data documentation](https://earthkit-data.readthedocs.io/)
- [DHIS2 Climate Tools](https://climate-tools.dhis2.org/)
