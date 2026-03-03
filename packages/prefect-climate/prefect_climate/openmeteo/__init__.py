"""Open-Meteo weather and air quality data."""

from prefect_climate.openmeteo.air_quality import fetch_daily_air_quality
from prefect_climate.openmeteo.client import fetch_openmeteo_hourly
from prefect_climate.openmeteo.forecast import fetch_daily_forecast
from prefect_climate.openmeteo.historical import fetch_daily_historical
from prefect_climate.openmeteo.schemas import DailyAirQuality, DailyWeather

__all__ = [
    "DailyAirQuality",
    "DailyWeather",
    "fetch_daily_air_quality",
    "fetch_daily_forecast",
    "fetch_daily_historical",
    "fetch_openmeteo_hourly",
]
