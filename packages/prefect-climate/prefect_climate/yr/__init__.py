"""yr.no Locationforecast weather data."""

from prefect_climate.yr.forecast import (
    LOCATIONFORECAST_URL,
    USER_AGENT,
    aggregate_forecast_daily,
    fetch_daily_forecasts,
    fetch_forecast,
)
from prefect_climate.yr.schemas import DailyForecast

__all__ = [
    "LOCATIONFORECAST_URL",
    "USER_AGENT",
    "DailyForecast",
    "aggregate_forecast_daily",
    "fetch_daily_forecasts",
    "fetch_forecast",
]
