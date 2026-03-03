"""Shared HTTP client for Open-Meteo APIs.

Open-Meteo provides free weather APIs with no authentication required.
This module handles the HTTP requests for all three endpoints:
historical weather, weather forecast, and air quality.

Reference: https://open-meteo.com/en/docs
"""

from __future__ import annotations

from typing import Any

import httpx

BASE_URLS = {
    "historical": "https://archive-api.open-meteo.com/v1/archive",
    "forecast": "https://api.open-meteo.com/v1/forecast",
    "air_quality": "https://air-quality-api.open-meteo.com/v1/air-quality",
}

USER_AGENT = "prefect-climate/1.0 github.com/mortenoh/prefect-examples"


def fetch_openmeteo_hourly(
    endpoint: str,
    lat: float,
    lon: float,
    hourly_params: list[str],
    start_date: str | None = None,
    end_date: str | None = None,
    extra_params: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Fetch hourly data from an Open-Meteo API endpoint.

    Args:
        endpoint: One of ``"historical"``, ``"forecast"``, or ``"air_quality"``.
        lat: Latitude in decimal degrees.
        lon: Longitude in decimal degrees.
        hourly_params: List of hourly variable names to request.
        start_date: Start date (YYYY-MM-DD). Required for historical.
        end_date: End date (YYYY-MM-DD). Required for historical.
        extra_params: Additional query parameters (e.g. ``wind_speed_unit``).

    Returns:
        Parsed JSON response from the Open-Meteo API.
    """
    url = BASE_URLS[endpoint]

    params: dict[str, str] = {
        "latitude": str(lat),
        "longitude": str(lon),
        "hourly": ",".join(hourly_params),
        "timezone": "UTC",
    }

    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    if extra_params:
        params.update(extra_params)

    response = httpx.get(
        url,
        params=params,
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
    )
    response.raise_for_status()
    data: dict[str, Any] = response.json()
    return data
