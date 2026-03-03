"""Open-Meteo Air Quality API client.

Fetches hourly air quality data from the Open-Meteo Air Quality API
and aggregates to daily values. Provides PM2.5, PM10, ozone, NO2,
SO2, CO, and European AQI.

Reference: https://open-meteo.com/en/docs/air-quality-api
"""

from __future__ import annotations

from collections import defaultdict
from statistics import mean

from prefect_climate.openmeteo.client import fetch_openmeteo_hourly
from prefect_climate.openmeteo.schemas import DailyAirQuality

AIR_QUALITY_PARAMS = [
    "pm2_5",
    "pm10",
    "ozone",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "carbon_monoxide",
    "european_aqi",
]


def fetch_daily_air_quality(lat: float, lon: float) -> list[DailyAirQuality]:
    """Fetch and aggregate daily air quality data for a given point.

    Fetches hourly air quality data from the Open-Meteo Air Quality API
    and aggregates to daily values. All pollutants use mean aggregation
    except ``european_aqi`` which uses max (worst-case daily index).

    Args:
        lat: Latitude in decimal degrees.
        lon: Longitude in decimal degrees.

    Returns:
        List of DailyAirQuality objects sorted by date.
    """
    data = fetch_openmeteo_hourly(
        endpoint="air_quality",
        lat=lat,
        lon=lon,
        hourly_params=AIR_QUALITY_PARAMS,
    )

    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        return []

    daily_pm2_5: dict[str, list[float]] = defaultdict(list)
    daily_pm10: dict[str, list[float]] = defaultdict(list)
    daily_ozone: dict[str, list[float]] = defaultdict(list)
    daily_no2: dict[str, list[float]] = defaultdict(list)
    daily_so2: dict[str, list[float]] = defaultdict(list)
    daily_co: dict[str, list[float]] = defaultdict(list)
    daily_aqi: dict[str, list[float]] = defaultdict(list)

    pm2_5_vals = hourly.get("pm2_5", [])
    pm10_vals = hourly.get("pm10", [])
    ozone_vals = hourly.get("ozone", [])
    no2_vals = hourly.get("nitrogen_dioxide", [])
    so2_vals = hourly.get("sulphur_dioxide", [])
    co_vals = hourly.get("carbon_monoxide", [])
    aqi_vals = hourly.get("european_aqi", [])

    for i, time_str in enumerate(times):
        date = time_str[:10]

        if i < len(pm2_5_vals) and pm2_5_vals[i] is not None:
            daily_pm2_5[date].append(pm2_5_vals[i])
        if i < len(pm10_vals) and pm10_vals[i] is not None:
            daily_pm10[date].append(pm10_vals[i])
        if i < len(ozone_vals) and ozone_vals[i] is not None:
            daily_ozone[date].append(ozone_vals[i])
        if i < len(no2_vals) and no2_vals[i] is not None:
            daily_no2[date].append(no2_vals[i])
        if i < len(so2_vals) and so2_vals[i] is not None:
            daily_so2[date].append(so2_vals[i])
        if i < len(co_vals) and co_vals[i] is not None:
            daily_co[date].append(co_vals[i])
        if i < len(aqi_vals) and aqi_vals[i] is not None:
            daily_aqi[date].append(aqi_vals[i])

    all_dates = sorted(
        set(daily_pm2_5)
        | set(daily_pm10)
        | set(daily_ozone)
        | set(daily_no2)
        | set(daily_so2)
        | set(daily_co)
        | set(daily_aqi)
    )

    results: list[DailyAirQuality] = []
    for date in all_dates:
        results.append(
            DailyAirQuality(
                date=date,
                pm2_5=round(mean(daily_pm2_5[date]), 1) if daily_pm2_5[date] else None,
                pm10=round(mean(daily_pm10[date]), 1) if daily_pm10[date] else None,
                ozone=round(mean(daily_ozone[date]), 1) if daily_ozone[date] else None,
                nitrogen_dioxide=round(mean(daily_no2[date]), 1) if daily_no2[date] else None,
                sulphur_dioxide=round(mean(daily_so2[date]), 1) if daily_so2[date] else None,
                carbon_monoxide=round(mean(daily_co[date]), 1) if daily_co[date] else None,
                european_aqi=round(max(daily_aqi[date]), 1) if daily_aqi[date] else None,
            )
        )

    return results
