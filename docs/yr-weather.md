# yr.no Weather Forecast Integration

This document describes the yr.no Locationforecast integration for
importing weather forecast data into DHIS2.

## Data source

[yr.no](https://www.yr.no/) is a free weather service operated by the
Norwegian Meteorological Institute (MET Norway). The
[Locationforecast 2.0](https://api.met.no/weatherapi/locationforecast/2.0/documentation)
API provides global point-based weather forecasts.

**Key characteristics:**

- **Endpoint:** `https://api.met.no/weatherapi/locationforecast/2.0/complete?lat={lat}&lon={lon}`
- **Authentication:** None. Requires a `User-Agent` header with app name and contact.
- **Coordinates:** Latitude and longitude, max 4 decimal places (5+ returns 403).
- **Forecast window:** Up to 9 days -- hourly for 0-48h, 6-hourly for 2-10 days.
- **Rate limit:** ~20 requests/second. Caching encouraged (respect `Expires` header).
- **No historical data:** The API only provides future forecasts. Running the
  import regularly builds a time series of forecast data over time in DHIS2.

## Variables

Six weather variables are extracted and aggregated to daily values:

| Variable | API field | Daily aggregation | Unit |
|----------|-----------|-------------------|------|
| Temperature | `air_temperature` | Mean | C |
| Precipitation | `precipitation_amount` | Sum | mm |
| Relative humidity | `relative_humidity` | Mean | % |
| Wind speed | `wind_speed` | Mean | m/s |
| Cloud cover | `cloud_area_fraction` | Mean | % |
| Air pressure | `air_pressure_at_sea_level` | Mean | hPa |

## Approach: point-based forecasts via centroids

Unlike CHIRPS and ERA5 (which provide raster data processed with zonal
statistics), yr.no returns point forecasts for specific coordinates.

The flow computes the **centroid** of each org unit polygon (mean of all
coordinate vertices) and queries yr.no for that single point. This is a
simple approximation that works well for org units where the weather is
relatively uniform across the area.

## DHIS2 data elements

| UID | Name | Description |
|-----|------|-------------|
| `WfYrTmpEst1` | WF: yr.no: Temperature | Daily mean air temperature (C) |
| `WfYrPrcEst1` | WF: yr.no: Precipitation | Daily total precipitation (mm) |
| `WfYrHumEst1` | WF: yr.no: Relative Humidity | Daily mean relative humidity (%) |
| `WfYrWndEst1` | WF: yr.no: Wind Speed | Daily mean wind speed (m/s) |
| `WfYrCldEst1` | WF: yr.no: Cloud Cover | Daily mean cloud area fraction (%) |
| `WfYrPrsEst1` | WF: yr.no: Air Pressure | Daily mean air pressure at sea level (hPa) |

**Data set:** `WfYrFrcSet1` -- "WF: yr.no: Weather Forecast", period type **Daily**.

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

## References

- [yr.no Locationforecast 2.0 documentation](https://api.met.no/weatherapi/locationforecast/2.0/documentation)
- [MET Norway terms of service](https://api.met.no/doc/TermsOfService)
- [GeoJSON forecast format](https://api.met.no/weatherapi/locationforecast/2.0/documentation#!/data/get_complete)
