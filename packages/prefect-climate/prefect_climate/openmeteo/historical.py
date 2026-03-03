"""Open-Meteo Historical Weather API client.

Fetches hourly historical weather data from the Open-Meteo Archive API
(1940-present) and aggregates to daily values. Uses the same centroid-based
point queries as yr.no but provides historical backfill capability.

Reference: https://open-meteo.com/en/docs/historical-weather-api
"""

from __future__ import annotations

import calendar
from collections import defaultdict
from statistics import mean
from typing import Any

from prefect_climate.openmeteo.client import fetch_openmeteo_hourly
from prefect_climate.openmeteo.schemas import DailyWeather

WEATHER_PARAMS = [
    "temperature_2m",
    "precipitation",
    "relative_humidity_2m",
    "wind_speed_10m",
    "cloud_cover",
    "pressure_msl",
]


def _aggregate_weather_daily(data: dict[str, Any]) -> list[DailyWeather]:
    """Aggregate hourly Open-Meteo weather data to daily values.

    Groups hourly values by date and computes:
    - Temperature: daily mean (Celsius)
    - Precipitation: daily sum (mm)
    - Humidity: daily mean (%)
    - Wind speed: daily mean (m/s)
    - Cloud cover: daily mean (%)
    - Pressure: daily mean (hPa)

    Args:
        data: Parsed JSON response from Open-Meteo API.

    Returns:
        List of DailyWeather objects sorted by date.
    """
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        return []

    daily_temps: dict[str, list[float]] = defaultdict(list)
    daily_precip: dict[str, list[float]] = defaultdict(list)
    daily_humidity: dict[str, list[float]] = defaultdict(list)
    daily_wind: dict[str, list[float]] = defaultdict(list)
    daily_cloud: dict[str, list[float]] = defaultdict(list)
    daily_pressure: dict[str, list[float]] = defaultdict(list)

    temps = hourly.get("temperature_2m", [])
    precips = hourly.get("precipitation", [])
    humidities = hourly.get("relative_humidity_2m", [])
    winds = hourly.get("wind_speed_10m", [])
    clouds = hourly.get("cloud_cover", [])
    pressures = hourly.get("pressure_msl", [])

    for i, time_str in enumerate(times):
        date = time_str[:10]  # YYYY-MM-DD

        if i < len(temps) and temps[i] is not None:
            daily_temps[date].append(temps[i])
        if i < len(precips) and precips[i] is not None:
            daily_precip[date].append(precips[i])
        if i < len(humidities) and humidities[i] is not None:
            daily_humidity[date].append(humidities[i])
        if i < len(winds) and winds[i] is not None:
            daily_wind[date].append(winds[i])
        if i < len(clouds) and clouds[i] is not None:
            daily_cloud[date].append(clouds[i])
        if i < len(pressures) and pressures[i] is not None:
            daily_pressure[date].append(pressures[i])

    all_dates = sorted(
        set(daily_temps)
        | set(daily_precip)
        | set(daily_humidity)
        | set(daily_wind)
        | set(daily_cloud)
        | set(daily_pressure)
    )

    results: list[DailyWeather] = []
    for date in all_dates:
        results.append(
            DailyWeather(
                date=date,
                temperature=round(mean(daily_temps[date]), 1) if daily_temps[date] else None,
                precipitation=round(sum(daily_precip[date]), 1) if daily_precip[date] else None,
                humidity=round(mean(daily_humidity[date]), 1) if daily_humidity[date] else None,
                wind_speed=round(mean(daily_wind[date]), 1) if daily_wind[date] else None,
                cloud_cover=round(mean(daily_cloud[date]), 1) if daily_cloud[date] else None,
                pressure=round(mean(daily_pressure[date]), 1) if daily_pressure[date] else None,
            )
        )

    return results


def fetch_daily_historical(
    lat: float,
    lon: float,
    year: int,
    months: list[int],
) -> list[DailyWeather]:
    """Fetch and aggregate daily historical weather for a given point.

    Computes start/end dates from year and months, fetches hourly data
    from the Open-Meteo Archive API, and aggregates to daily values.

    Args:
        lat: Latitude in decimal degrees.
        lon: Longitude in decimal degrees.
        year: Data year.
        months: List of month numbers (1-12).

    Returns:
        List of DailyWeather objects sorted by date.
    """
    first_month = min(months)
    last_month = max(months)
    start_date = f"{year}-{first_month:02d}-01"
    last_day = calendar.monthrange(year, last_month)[1]
    end_date = f"{year}-{last_month:02d}-{last_day:02d}"

    data = fetch_openmeteo_hourly(
        endpoint="historical",
        lat=lat,
        lon=lon,
        hourly_params=WEATHER_PARAMS,
        start_date=start_date,
        end_date=end_date,
        extra_params={"wind_speed_unit": "ms"},
    )

    return _aggregate_weather_daily(data)
