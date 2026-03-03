"""Open-Meteo Weather Forecast API client.

Fetches hourly weather forecast data from the Open-Meteo Forecast API
(up to 16 days) and aggregates to daily values. Provides an alternative
to yr.no with multiple model support.

Reference: https://open-meteo.com/en/docs
"""

from __future__ import annotations

from prefect_climate.openmeteo.client import fetch_openmeteo_hourly
from prefect_climate.openmeteo.historical import WEATHER_PARAMS, _aggregate_weather_daily
from prefect_climate.openmeteo.schemas import DailyWeather


def fetch_daily_forecast(lat: float, lon: float) -> list[DailyWeather]:
    """Fetch and aggregate a daily weather forecast for a given point.

    Fetches the full ~16-day forecast window from the Open-Meteo
    Forecast API and aggregates hourly data to daily values.

    Args:
        lat: Latitude in decimal degrees.
        lon: Longitude in decimal degrees.

    Returns:
        List of DailyWeather objects sorted by date.
    """
    data = fetch_openmeteo_hourly(
        endpoint="forecast",
        lat=lat,
        lon=lon,
        hourly_params=WEATHER_PARAMS,
        extra_params={"wind_speed_unit": "ms"},
    )

    return _aggregate_weather_daily(data)
