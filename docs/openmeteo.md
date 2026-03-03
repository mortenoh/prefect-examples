# Open-Meteo Weather and Air Quality

Open-Meteo is a free, open-source weather API that provides global weather
forecasts, historical weather data, and air quality information. No API key
is required and the service allows up to 10,000 API calls per day for
non-commercial use.

## Key characteristics

### Historical Weather API

| Property | Value |
|----------|-------|
| Provider | Open-Meteo (open-source) |
| Spatial resolution | Point-based (single lat/lon coordinate) |
| Temporal resolution | Hourly |
| Coverage | Global |
| Time range | 1940 -- present |
| Format | JSON |
| CRS | EPSG:4326 (WGS 84) |

### Weather Forecast API

| Property | Value |
|----------|-------|
| Provider | Open-Meteo (open-source) |
| Spatial resolution | Point-based (single lat/lon coordinate) |
| Temporal resolution | Hourly |
| Coverage | Global |
| Forecast window | Up to 16 days |
| Format | JSON |
| CRS | EPSG:4326 (WGS 84) |

### Air Quality API

| Property | Value |
|----------|-------|
| Provider | Open-Meteo (open-source, CAMS data) |
| Spatial resolution | Point-based (single lat/lon coordinate) |
| Temporal resolution | Hourly |
| Coverage | Global |
| Forecast window | Current + ~5 days |
| Format | JSON |
| CRS | EPSG:4326 (WGS 84) |

## API endpoints

| Endpoint | Base URL | Description |
|----------|----------|-------------|
| Historical | `https://archive-api.open-meteo.com/v1/archive` | Weather data 1940--present |
| Forecast | `https://api.open-meteo.com/v1/forecast` | Weather forecast up to 16 days |
| Air Quality | `https://air-quality-api.open-meteo.com/v1/air-quality` | Air quality current + forecast |

All endpoints accept the same core parameters:

```
?latitude={lat}&longitude={lon}&hourly={variables}&timezone=UTC
```

The historical endpoint additionally requires `start_date` and `end_date`
parameters in `YYYY-MM-DD` format.

## Response format

All endpoints return JSON with an `hourly` object containing parallel arrays
of timestamps and variable values:

```json
{
  "latitude": 8.5,
  "longitude": -13.25,
  "hourly": {
    "time": [
      "2024-01-15T00:00",
      "2024-01-15T01:00",
      "2024-01-15T02:00"
    ],
    "temperature_2m": [24.1, 23.8, 23.5],
    "precipitation": [0.0, 0.1, 0.0],
    "relative_humidity_2m": [82, 84, 85]
  }
}
```

Missing values are represented as `null` in the JSON arrays.

## Variable catalog

### Weather variables (historical + forecast)

| API parameter | Description | Unit |
|---------------|-------------|------|
| `temperature_2m` | Air temperature at 2 metres | Celsius |
| `precipitation` | Total precipitation (rain + snow) | mm |
| `relative_humidity_2m` | Relative humidity at 2 metres | % |
| `wind_speed_10m` | Wind speed at 10 metres | m/s |
| `cloud_cover` | Total cloud cover | % |
| `pressure_msl` | Mean sea-level pressure | hPa |

### Air quality variables

| API parameter | Description | Unit |
|---------------|-------------|------|
| `pm2_5` | Fine particulate matter (diameter < 2.5 um) | ug/m3 |
| `pm10` | Coarse particulate matter (diameter < 10 um) | ug/m3 |
| `ozone` | Ground-level ozone (O3) | ug/m3 |
| `nitrogen_dioxide` | Nitrogen dioxide (NO2) | ug/m3 |
| `sulphur_dioxide` | Sulphur dioxide (SO2) | ug/m3 |
| `carbon_monoxide` | Carbon monoxide (CO) | ug/m3 |
| `european_aqi` | European Air Quality Index | index (0--500+) |

## Daily aggregation

The pipeline requests hourly data and aggregates to daily values:

| Variable | Aggregation | Rationale |
|----------|-------------|-----------|
| Temperature | Mean | Representative daily temperature |
| Precipitation | Sum | Total daily accumulation |
| Humidity | Mean | Average daily moisture |
| Wind speed | Mean | Average daily wind conditions |
| Cloud cover | Mean | Average daily sky conditions |
| Pressure | Mean | Average daily atmospheric pressure |
| PM2.5 | Mean | Average daily exposure |
| PM10 | Mean | Average daily exposure |
| Ozone | Mean | Average daily concentration |
| NO2 | Mean | Average daily concentration |
| SO2 | Mean | Average daily concentration |
| CO | Mean | Average daily concentration |
| European AQI | Max | Worst-case daily air quality |

The European AQI uses `max()` rather than `mean()` because the index
represents the worst air quality condition during the day, which is the
standard reporting convention.

## Centroid approach

Open-Meteo APIs accept a single (latitude, longitude) point. For each
DHIS2 organisation unit with polygon geometry, the pipeline:

1. Computes the centroid of the polygon (mean of all vertex coordinates)
2. Queries the Open-Meteo API with that centroid
3. Assigns the result to the entire org unit

This is the same approach used by the yr.no integration.

## DHIS2 data model

### Data sets (3)

| UID | Name | Period type | Future periods |
|-----|------|------------|----------------|
| `OmHwDlySet1` | PR: OM: WH: Historical Weather | Daily | 0 |
| `OmFcDlySet1` | PR: OM: WF: Weather Forecast | Daily | 17 |
| `OmAqDlySet1` | PR: OM: AQ: Air Quality | Daily | 6 |

### Data elements -- Historical Weather (6)

| UID | Name | Short name |
|-----|------|------------|
| `OmHwTmpEst1` | PR: OM: WH: Temperature | PR: OM: WH: Temp |
| `OmHwPrcEst1` | PR: OM: WH: Precipitation | PR: OM: WH: Precip |
| `OmHwHumEst1` | PR: OM: WH: Relative Humidity | PR: OM: WH: Rel Humidity |
| `OmHwWndEst1` | PR: OM: WH: Wind Speed | PR: OM: WH: Wind Speed |
| `OmHwCldEst1` | PR: OM: WH: Cloud Cover | PR: OM: WH: Cloud Cover |
| `OmHwPrsEst1` | PR: OM: WH: Air Pressure | PR: OM: WH: Air Pressure |

### Data elements -- Weather Forecast (6)

| UID | Name | Short name |
|-----|------|------------|
| `OmFcTmpEst1` | PR: OM: WF: Temperature | PR: OM: WF: Temp |
| `OmFcPrcEst1` | PR: OM: WF: Precipitation | PR: OM: WF: Precip |
| `OmFcHumEst1` | PR: OM: WF: Relative Humidity | PR: OM: WF: Rel Humidity |
| `OmFcWndEst1` | PR: OM: WF: Wind Speed | PR: OM: WF: Wind Speed |
| `OmFcCldEst1` | PR: OM: WF: Cloud Cover | PR: OM: WF: Cloud Cover |
| `OmFcPrsEst1` | PR: OM: WF: Air Pressure | PR: OM: WF: Air Pressure |

### Data elements -- Air Quality (7)

| UID | Name | Short name |
|-----|------|------------|
| `OmAqPm2Est1` | PR: OM: AQ: PM2.5 | PR: OM: AQ: PM2.5 |
| `OmAqP10Est1` | PR: OM: AQ: PM10 | PR: OM: AQ: PM10 |
| `OmAqO3xEst1` | PR: OM: AQ: Ozone | PR: OM: AQ: Ozone |
| `OmAqNo2Est1` | PR: OM: AQ: Nitrogen Dioxide | PR: OM: AQ: NO2 |
| `OmAqSo2Est1` | PR: OM: AQ: Sulphur Dioxide | PR: OM: AQ: SO2 |
| `OmAqCoxEst1` | PR: OM: AQ: Carbon Monoxide | PR: OM: AQ: CO |
| `OmAqAqiEst1` | PR: OM: AQ: European AQI | PR: OM: AQ: EU AQI |

## Health relevance

### Weather data

Historical and forecast weather data supports disease surveillance models:

- **Temperature** and **humidity** are key predictors for vector-borne
  diseases (malaria, dengue) and respiratory infections
- **Precipitation** drives water-borne disease risk and flooding events
- **Wind speed** affects pollutant dispersion and vector movement
- Historical data (1940--present) enables long-term climate-health analysis

### Air quality data

Air pollution is a leading environmental health risk globally:

- **PM2.5** is the most health-relevant pollutant (WHO limit: 15 ug/m3 daily)
- **PM10** causes respiratory and cardiovascular effects
- **Ozone** triggers asthma and reduces lung function
- **NO2** and **SO2** are markers of traffic and industrial pollution
- **CO** is toxic at high concentrations
- **European AQI** provides a single composite health risk indicator

## Comparison with other data sources

| Feature | Open-Meteo | yr.no | ERA5-Land | CHIRPS |
|---------|-----------|-------|-----------|--------|
| Historical data | 1940--present | None | 1950--present | 1981--present |
| Forecast | 16 days | ~9 days | None | None |
| Air quality | Yes (7 variables) | No | No | No |
| Spatial type | Point-based | Point-based | Gridded (~9 km) | Gridded (~5 km) |
| API key | None | None (User-Agent) | CDS API key | None |
| Rate limit | 10k calls/day | Fair use | Quota-based | None |
| Weather variables | 6 | 6 | 7 | 1 (precip only) |
| Period type | Daily | Daily | Monthly | Monthly |
| Zonal stats | No (centroid) | No (centroid) | Yes | Yes |

## Running the flows

### Historical weather

```bash
# Import 2024 weather for all months
uv run python flows/dhis2/dhis2_openmeteo_historical_import.py
```

Uses `query.year` and `query.months` to select the time window.

### Weather forecast

```bash
# Import current 16-day forecast
uv run python flows/dhis2/dhis2_openmeteo_forecast_import.py
```

Ignores `query.year`/`query.months` -- always fetches the current window.

### Air quality

```bash
# Import current + 5-day air quality forecast
uv run python flows/dhis2/dhis2_openmeteo_air_quality_import.py
```

Ignores `query.year`/`query.months` -- always fetches the current window.

## Direct API usage

```python
from prefect_climate.openmeteo import (
    fetch_daily_forecast,
    fetch_daily_historical,
    fetch_daily_air_quality,
)

# Weather forecast (16 days)
forecast = fetch_daily_forecast(lat=8.48, lon=-13.23)
for day in forecast[:3]:
    print(f"{day.date}: {day.temperature}C, {day.precipitation}mm")

# Historical weather (specific year/months)
historical = fetch_daily_historical(lat=8.48, lon=-13.23, year=2024, months=[1, 2, 3])
for day in historical[:3]:
    print(f"{day.date}: {day.temperature}C, {day.precipitation}mm")

# Air quality
air_quality = fetch_daily_air_quality(lat=8.48, lon=-13.23)
for day in air_quality[:3]:
    print(f"{day.date}: PM2.5={day.pm2_5}, AQI={day.european_aqi}")
```

## References

- [Open-Meteo documentation](https://open-meteo.com/en/docs)
- [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)
- [Open-Meteo Air Quality API](https://open-meteo.com/en/docs/air-quality-api)
- [European Air Quality Index](https://www.eea.europa.eu/themes/air/air-quality-index)
- [WHO Air Quality Guidelines](https://www.who.int/news-room/fact-sheets/detail/ambient-(outdoor)-air-quality-and-health)
- [CAMS (Copernicus Atmosphere Monitoring Service)](https://atmosphere.copernicus.eu/)
