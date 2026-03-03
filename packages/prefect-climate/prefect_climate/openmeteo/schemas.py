"""Data models for Open-Meteo weather and air quality data."""

from __future__ import annotations

from pydantic import BaseModel, Field


class DailyWeather(BaseModel):
    """Aggregated daily weather data from Open-Meteo."""

    date: str = Field(description="Date (YYYY-MM-DD)")
    temperature: float | None = Field(default=None, description="Mean air temperature (Celsius)")
    precipitation: float | None = Field(default=None, description="Total precipitation (mm)")
    humidity: float | None = Field(default=None, description="Mean relative humidity (%)")
    wind_speed: float | None = Field(default=None, description="Mean wind speed (m/s)")
    cloud_cover: float | None = Field(default=None, description="Mean cloud cover (%)")
    pressure: float | None = Field(default=None, description="Mean sea-level pressure (hPa)")


class DailyAirQuality(BaseModel):
    """Aggregated daily air quality data from Open-Meteo."""

    date: str = Field(description="Date (YYYY-MM-DD)")
    pm2_5: float | None = Field(default=None, description="Mean PM2.5 (ug/m3)")
    pm10: float | None = Field(default=None, description="Mean PM10 (ug/m3)")
    ozone: float | None = Field(default=None, description="Mean ozone (ug/m3)")
    nitrogen_dioxide: float | None = Field(default=None, description="Mean NO2 (ug/m3)")
    sulphur_dioxide: float | None = Field(default=None, description="Mean SO2 (ug/m3)")
    carbon_monoxide: float | None = Field(default=None, description="Mean CO (ug/m3)")
    european_aqi: float | None = Field(default=None, description="Max European AQI (index)")
