# yr.no Weather Forecast

yr.no is a free weather service operated by the Norwegian Meteorological
Institute (MET Norway). The Locationforecast 2.0 API provides global
point-based weather forecasts in GeoJSON format. No API key is required --
only a `User-Agent` header identifying the application.

## Key characteristics

| Property | Value |
|----------|-------|
| Provider | MET Norway (Norwegian Meteorological Institute) |
| Spatial resolution | Point-based (single lat/lon coordinate) |
| Temporal resolution | Hourly (0-48h), 6-hourly (2-10 days) |
| Coverage | Global |
| Forecast window | Up to ~9 days from current time |
| Historical data | None -- forecast only |
| Format | GeoJSON (RFC 7946) |
| CRS | EPSG:4326 (WGS 84) |

## API endpoints

The Locationforecast 2.0 API offers three endpoints:

| Endpoint | Description |
|----------|-------------|
| `/compact` | Subset of variables sufficient for most use cases |
| `/complete` | Full set of variables including percentiles and cloud layers |
| `/status` | API health check |

The pipeline uses the **complete** endpoint to access all available
variables:

```
https://api.met.no/weatherapi/locationforecast/2.0/complete?lat={lat}&lon={lon}
```

## API requirements

### User-Agent header

A custom `User-Agent` header is **mandatory**. It must contain the
application name and contact information (GitHub URL, email, or website).
Missing or generic User-Agent strings return `403 Forbidden`. Falsifying
this information risks permanent blacklisting.

The pipeline sets:

```
User-Agent: prefect-climate/1.0 github.com/mortenoh/prefect-examples
```

### Coordinate precision

Coordinates must use **no more than 4 decimal places**. Using 5 or more
decimals returns `403 Forbidden`. This restriction enables effective
server-side caching. Four decimal places give ~11 m precision, which is
more than sufficient for weather data.

```python
# Coordinates are truncated to 4 decimals before the API call
lat = round(lat, 4)  # e.g. 8.4842
lon = round(lon, 4)  # e.g. -13.2344
```

### HTTPS

Always use HTTPS. Unencrypted HTTP traffic may be throttled or blocked.

### Caching and conditional requests

MET Norway strongly encourages caching. Every response includes:

- **`Expires`** -- when the forecast data will next be updated. Do not
  re-request before this time.
- **`Last-Modified`** -- when the data was last changed.

Use `If-Modified-Since` with the cached `Last-Modified` value on
subsequent requests. A `304 Not Modified` response means the cached data
is still current.

### Rate limiting

No hard published limit, but approximately **20 requests/second** is the
practical ceiling. Monitor for `429 Too Many Requests` responses. The
pipeline processes org units sequentially, which naturally stays well
within limits.

### Status codes

| Code | Meaning |
|------|---------|
| 200 | Success, new data returned |
| 304 | Not Modified -- use cached data |
| 403 | Forbidden -- bad User-Agent or coordinate precision |
| 429 | Throttled -- reduce request rate |

## Response format

The API returns a GeoJSON FeatureCollection with a `timeseries` array.
Each entry has an ISO 8601 timestamp and nested data:

```json
{
  "type": "Feature",
  "geometry": { "type": "Point", "coordinates": [-13.2344, 8.4842] },
  "properties": {
    "timeseries": [
      {
        "time": "2026-03-03T12:00:00Z",
        "data": {
          "instant": {
            "details": {
              "air_temperature": 28.5,
              "air_pressure_at_sea_level": 1010.2,
              "relative_humidity": 68.0,
              "wind_speed": 2.3,
              "cloud_area_fraction": 45.0,
              "dew_point_temperature": 22.1,
              "wind_from_direction": 210.5
            }
          },
          "next_1_hours": {
            "summary": { "symbol_code": "partlycloudy_day" },
            "details": { "precipitation_amount": 0.0 }
          },
          "next_6_hours": {
            "summary": { "symbol_code": "partlycloudy_day" },
            "details": {
              "precipitation_amount": 0.2,
              "air_temperature_max": 30.1,
              "air_temperature_min": 27.8
            }
          }
        }
      }
    ]
  }
}
```

- **`instant.details`** -- atmospheric variables at the exact timestamp.
- **`next_1_hours.details`** -- accumulated values for the next 1 hour
  (available for first ~48 hours).
- **`next_6_hours.details`** -- accumulated values for the next 6 hours
  (available for the full forecast window).

## Available variables

### Instant variables

Variables marked with **compact** are included in both `/compact` and
`/complete`; others are `/complete` only.

| Variable | Unit | Compact | Description |
|----------|------|---------|-------------|
| `air_temperature` | C | yes | Air temperature at 2 m height |
| `air_temperature_percentile_10` | C | no | 10th percentile air temperature |
| `air_temperature_percentile_90` | C | no | 90th percentile air temperature |
| `air_pressure_at_sea_level` | hPa | yes | Air pressure reduced to sea level |
| `cloud_area_fraction` | % | yes | Total cloud cover (all heights) |
| `cloud_area_fraction_high` | % | no | Cloud cover above 5000 m |
| `cloud_area_fraction_medium` | % | no | Cloud cover 2000-5000 m |
| `cloud_area_fraction_low` | % | no | Cloud cover below 2000 m |
| `dew_point_temperature` | C | no | Dewpoint temperature at 2 m |
| `fog_area_fraction` | % | no | Area covered in fog (visibility < 1 km) |
| `relative_humidity` | % | yes | Relative humidity at 2 m |
| `ultraviolet_index_clear_sky` | index | no | UV index for cloud-free conditions (0-11+) |
| `wind_from_direction` | degrees | yes | Direction wind is coming from (0 = N, 90 = E) |
| `wind_speed` | m/s | yes | Wind speed at 10 m (10-minute average) |
| `wind_speed_percentile_10` | m/s | no | 10th percentile wind speed |
| `wind_speed_percentile_90` | m/s | no | 90th percentile wind speed |
| `wind_speed_of_gust` | m/s | no | Maximum gust at 10 m |

### Period variables

| Variable | Unit | Period | Description |
|----------|------|--------|-------------|
| `precipitation_amount` | mm | 1h, 6h | Expected precipitation for period |
| `precipitation_amount_max` | mm | 1h, 6h | Maximum likely precipitation |
| `precipitation_amount_min` | mm | 1h, 6h | Minimum likely precipitation |
| `probability_of_precipitation` | % | 1h, 6h, 12h | Chance of precipitation |
| `probability_of_thunder` | % | 1h, 6h | Chance of thunder |
| `air_temperature_max` | C | 6h | Maximum air temperature over period |
| `air_temperature_min` | C | 6h | Minimum air temperature over period |
| `symbol_code` | string | 1h, 6h, 12h | Weather icon code |

Variable names follow the international
[CF Standard Name](https://cfconventions.org/Data/cf-standard-names/current/build/cf-standard-name-table.html)
vocabulary (required by the EU INSPIRE directive).

## Variables used in the pipeline

Six variables are extracted from the `/complete` response and aggregated
from hourly/6-hourly to **daily** values:

| Variable | API field | Source | Daily aggregation | Unit |
|----------|-----------|--------|-------------------|------|
| Temperature | `air_temperature` | instant | Mean of all hourly values | C |
| Precipitation | `precipitation_amount` | next_1_hours / next_6_hours | Sum of all periods | mm |
| Relative humidity | `relative_humidity` | instant | Mean of all hourly values | % |
| Wind speed | `wind_speed` | instant | Mean of all hourly values | m/s |
| Cloud cover | `cloud_area_fraction` | instant | Mean of all hourly values | % |
| Air pressure | `air_pressure_at_sea_level` | instant | Mean of all hourly values | hPa |

### Daily aggregation logic

For each calendar day (UTC), the pipeline:

1. Groups all timeseries entries by date.
2. For **instant variables** (temperature, humidity, wind speed, cloud
   cover, pressure): computes the arithmetic **mean** of all hourly values
   within the day.
3. For **precipitation**: **sums** the `precipitation_amount` values.
   Prefers `next_1_hours` (higher resolution) when available; falls back
   to `next_6_hours` for the later part of the forecast window.

Days with no data for a given variable produce `None` (skipped during
DHIS2 import).

## Approach: point-based forecasts via centroids

Unlike CHIRPS and ERA5 (which provide raster data processed with zonal
statistics), yr.no returns point forecasts for specific coordinates.

The flow computes the **centroid** of each org unit polygon (mean of all
coordinate vertices) and queries yr.no for that single point. This is a
simple approximation that works well for org units where the weather is
relatively uniform across the area.

```python
from prefect_climate import centroid

lat, lon = centroid(org_unit.geometry)
# e.g. (8.4842, -13.2344) for Bo District, Sierra Leone
```

For large or irregularly shaped org units, the centroid may not be
representative of conditions across the entire area. Consider this
limitation when interpreting data for mountainous regions or coastal
boundaries.

## Pipeline overview

1. Fetch org unit geometries from DHIS2 (Polygon/MultiPolygon only)
2. Ensure 6 data elements and 1 daily data set exist in DHIS2
3. For each org unit:
   a. Compute polygon centroid (mean lat/lon)
   b. Truncate coordinates to 4 decimal places
   c. Fetch forecast from yr.no `/complete` endpoint
   d. Aggregate hourly timeseries to daily values
4. Build DHIS2 data values (period format: `YYYYMMDD`)
5. POST data values to DHIS2 `/api/dataValueSets`
6. Create markdown artifact with import summary

```python
@flow(name="dhis2_yr_weather_import", log_prints=True)
def dhis2_yr_weather_import_flow(query: ClimateQuery | None = None) -> ImportResult:
    client = get_dhis2_credentials().get_client()
    org_units = ensure_dhis2_metadata(client, query.org_unit_level)
    forecast_results = []
    for ou in org_units:
        result = fetch_org_unit_forecast(ou)  # centroid + yr.no API
        forecast_results.append(result)
    data_values = build_data_values(forecast_results)
    return import_to_dhis2(client, dhis2_url, org_units, data_values)
```

## DHIS2 data model

```
Data Set: WF: yr.no: Weather Forecast (WfYrFrcSet1)  [Daily, openFuturePeriods=10]
  |- Data Element: WF: yr.no: Temperature       (WfYrTmpEst1)  -- mean C
  |- Data Element: WF: yr.no: Precipitation      (WfYrPrcEst1)  -- sum mm
  |- Data Element: WF: yr.no: Relative Humidity   (WfYrHumEst1)  -- mean %
  |- Data Element: WF: yr.no: Wind Speed          (WfYrWndEst1)  -- mean m/s
  |- Data Element: WF: yr.no: Cloud Cover         (WfYrCldEst1)  -- mean %
  |- Data Element: WF: yr.no: Air Pressure        (WfYrPrsEst1)  -- mean hPa
```

| UID | Name | Aggregation | Unit |
|-----|------|-------------|------|
| `WfYrTmpEst1` | WF: yr.no: Temperature | Daily mean | degrees Celsius (C) |
| `WfYrPrcEst1` | WF: yr.no: Precipitation | Daily sum | millimetres (mm) |
| `WfYrHumEst1` | WF: yr.no: Relative Humidity | Daily mean | percent (%) |
| `WfYrWndEst1` | WF: yr.no: Wind Speed | Daily mean | metres per second (m/s) |
| `WfYrCldEst1` | WF: yr.no: Cloud Cover | Daily mean | percent (%) |
| `WfYrPrsEst1` | WF: yr.no: Air Pressure | Daily mean | hectopascals (hPa) |

The data set uses `openFuturePeriods=10` since forecasts extend ~9 days
into the future.

### Building a time series

The yr.no API provides only **future forecasts** -- there is no historical
archive. Each run of the flow imports the current ~9-day forecast window.
Running the flow on a regular schedule (e.g. daily via cron) builds up a
historical record of forecast data in DHIS2 over time. Overlapping
forecast days from consecutive runs are updated in place.

## Health relevance

| Variable | Health relevance |
|----------|-----------------|
| **Temperature** | Heat-related illness, malaria transmission windows (optimal 20-30 C) |
| **Precipitation** | Waterborne disease risk, flooding, mosquito breeding habitat |
| **Relative humidity** | Pathogen survival and airborne transmission, respiratory illness |
| **Wind speed** | Disease vector dispersal, air quality, extreme weather preparedness |
| **Cloud cover** | UV exposure estimation, solar energy availability |
| **Air pressure** | Migraine triggers, respiratory conditions, storm prediction |

## Comparison with ERA5 and CHIRPS

| Property | yr.no | ERA5-Land | CHIRPS |
|----------|-------|-----------|--------|
| Data type | Forecast (future) | Reanalysis (historical) | Observation (historical) |
| Spatial | Point-based (centroid) | ~9 km raster (zonal stats) | ~5 km raster (zonal stats) |
| Temporal | Daily (from hourly) | Monthly | Monthly (from daily) |
| Variables | 6 weather indicators | 7 climate indicators | Precipitation only |
| API key | None (User-Agent only) | CDS API key required | None (direct download) |
| Latency | Real-time forecast | ~5 days behind present | ~3 weeks behind present |

yr.no complements ERA5 and CHIRPS by providing **near-term forecast data**
rather than historical observations. This enables early warning and
preparedness workflows in DHIS2.

## Running the flow

The flow is intended for manual or scheduled runs. Each run fetches the
current forecast window (~9 days) and imports it into DHIS2.

```bash
# Direct run
uv run python flows/dhis2/dhis2_yr_weather_import.py

# Via Docker deployment
docker compose build dhis2-yr-weather-docker
docker compose up -d dhis2-yr-weather-docker
```

### Direct API usage

```python
from prefect_climate.yr import fetch_daily_forecasts

# Fetch forecast for Freetown, Sierra Leone
forecasts = fetch_daily_forecasts(lat=8.48, lon=-13.23)
for day in forecasts:
    print(f"{day.date}: {day.temperature}C, {day.precipitation}mm rain")
```

```python
from prefect_climate.yr import fetch_forecast, aggregate_forecast_daily

# Two-step: fetch raw response, then aggregate
raw = fetch_forecast(lat=8.48, lon=-13.23)
daily = aggregate_forecast_daily(raw)
```

## References

- [yr.no Locationforecast 2.0 documentation](https://api.met.no/weatherapi/locationforecast/2.0/documentation)
- [Locationforecast data model](https://docs.api.met.no/doc/locationforecast/datamodel.html)
- [Locationforecast HOWTO](https://docs.api.met.no/doc/locationforecast/HowTO.html)
- [MET Norway terms of service](https://api.met.no/doc/TermsOfService)
- [CF Standard Names](https://cfconventions.org/Data/cf-standard-names/current/build/cf-standard-name-table.html)
- [DHIS2 Climate Tools](https://climate-tools.dhis2.org/)
