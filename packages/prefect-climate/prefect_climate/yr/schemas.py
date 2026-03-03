"""Data models for yr.no weather forecast data."""

from __future__ import annotations

from pydantic import BaseModel, Field


class DailyForecast(BaseModel):
    """Aggregated daily weather forecast from yr.no Locationforecast."""

    date: str = Field(description="Forecast date (YYYY-MM-DD)")
    temperature: float | None = Field(default=None, description="Mean air temperature (Celsius)")
    precipitation: float | None = Field(default=None, description="Total precipitation (mm)")
    humidity: float | None = Field(default=None, description="Mean relative humidity (%)")
    wind_speed: float | None = Field(default=None, description="Mean wind speed (m/s)")
    cloud_cover: float | None = Field(default=None, description="Mean cloud area fraction (%)")
    pressure: float | None = Field(default=None, description="Mean air pressure at sea level (hPa)")
