"""yr.no Locationforecast 2.0 client.

Fetches point-based weather forecasts from the MET Norway Locationforecast
API and aggregates hourly data to daily values. No API key required -- only
a ``User-Agent`` header identifying the application.

Reference: https://api.met.no/weatherapi/locationforecast/2.0/documentation
"""

from __future__ import annotations

from collections import defaultdict
from statistics import mean
from typing import Any

import httpx

from prefect_climate.yr.schemas import DailyForecast

LOCATIONFORECAST_URL = "https://api.met.no/weatherapi/locationforecast/2.0/complete"
USER_AGENT = "prefect-climate/1.0 github.com/mortenoh/prefect-examples"


def fetch_forecast(lat: float, lon: float) -> dict[str, Any]:
    """Fetch a complete weather forecast for a given point.

    Truncates coordinates to 4 decimal places (yr.no returns 403 for
    higher precision). Sets the required ``User-Agent`` header.

    Args:
        lat: Latitude in decimal degrees.
        lon: Longitude in decimal degrees.

    Returns:
        Parsed JSON response from the Locationforecast API.
    """
    lat = round(lat, 4)
    lon = round(lon, 4)

    response = httpx.get(
        LOCATIONFORECAST_URL,
        params={"lat": lat, "lon": lon},
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
    )
    response.raise_for_status()
    data: dict[str, Any] = response.json()
    return data


def aggregate_forecast_daily(forecast: dict[str, Any]) -> list[DailyForecast]:
    """Aggregate hourly/6-hourly forecast data to daily values.

    Groups the timeseries entries by date and computes:
    - Temperature: daily mean (Celsius)
    - Precipitation: daily sum (mm)
    - Humidity: daily mean (%)
    - Wind speed: daily mean (m/s)
    - Cloud cover: daily mean (%)
    - Pressure: daily mean (hPa)

    Args:
        forecast: Parsed JSON from :func:`fetch_forecast`.

    Returns:
        List of :class:`DailyForecast` objects sorted by date.
    """
    timeseries = forecast.get("properties", {}).get("timeseries", [])

    daily_temps: dict[str, list[float]] = defaultdict(list)
    daily_precip: dict[str, list[float]] = defaultdict(list)
    daily_humidity: dict[str, list[float]] = defaultdict(list)
    daily_wind: dict[str, list[float]] = defaultdict(list)
    daily_cloud: dict[str, list[float]] = defaultdict(list)
    daily_pressure: dict[str, list[float]] = defaultdict(list)

    for entry in timeseries:
        time_str = entry.get("time", "")
        date = time_str[:10]  # YYYY-MM-DD
        if not date:
            continue

        instant = entry.get("data", {}).get("instant", {}).get("details", {})

        if "air_temperature" in instant:
            daily_temps[date].append(instant["air_temperature"])
        if "relative_humidity" in instant:
            daily_humidity[date].append(instant["relative_humidity"])
        if "wind_speed" in instant:
            daily_wind[date].append(instant["wind_speed"])
        if "cloud_area_fraction" in instant:
            daily_cloud[date].append(instant["cloud_area_fraction"])
        if "air_pressure_at_sea_level" in instant:
            daily_pressure[date].append(instant["air_pressure_at_sea_level"])

        # Precipitation is in the next_1_hours or next_6_hours summary
        for period_key in ("next_1_hours", "next_6_hours"):
            precip_data = entry.get("data", {}).get(period_key, {}).get("details", {})
            if "precipitation_amount" in precip_data:
                daily_precip[date].append(precip_data["precipitation_amount"])
                break  # prefer next_1_hours; only use next_6_hours as fallback

    all_dates = sorted(
        set(daily_temps)
        | set(daily_precip)
        | set(daily_humidity)
        | set(daily_wind)
        | set(daily_cloud)
        | set(daily_pressure)
    )

    results: list[DailyForecast] = []
    for date in all_dates:
        results.append(
            DailyForecast(
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


def fetch_daily_forecasts(lat: float, lon: float) -> list[DailyForecast]:
    """Fetch and aggregate a daily weather forecast for a given point.

    Convenience function combining :func:`fetch_forecast` and
    :func:`aggregate_forecast_daily`.

    Args:
        lat: Latitude in decimal degrees.
        lon: Longitude in decimal degrees.

    Returns:
        List of :class:`DailyForecast` objects sorted by date.
    """
    raw = fetch_forecast(lat, lon)
    return aggregate_forecast_daily(raw)
