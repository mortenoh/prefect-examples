"""Multi-Source Forecast.

Chained .map() calls: geocode cities then fetch forecasts using the
coordinates. Demonstrates multi-step simulated API orchestration.

Airflow equivalent: Multi-city forecast, geocoding (DAGs 081, 086).
Prefect approach:    Chained .map() with deterministic simulation.
"""

from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class CityCoordinates(BaseModel):
    """Geocoded city with lat/lon."""

    city: str
    lat: float
    lon: float


class WeatherForecast(BaseModel):
    """Forecast for a single city."""

    city: str
    temperature: float
    humidity: float
    condition: str


class ForecastSummary(BaseModel):
    """Aggregated forecast summary."""

    city_count: int
    warmest: str
    coldest: str
    avg_temperature: float
    forecasts: list[WeatherForecast]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def geocode_city(city: str) -> CityCoordinates:
    """Simulate geocoding a city name to coordinates.

    Args:
        city: City name.

    Returns:
        CityCoordinates with deterministic lat/lon.
    """
    coords = {
        "London": (51.5, -0.1),
        "Paris": (48.9, 2.3),
        "Tokyo": (35.7, 139.7),
        "New York": (40.7, -74.0),
        "Sydney": (-33.9, 151.2),
    }
    lat, lon = coords.get(city, (0.0, 0.0))
    print(f"Geocoded {city}: ({lat}, {lon})")
    return CityCoordinates(city=city, lat=lat, lon=lon)


@task
def fetch_forecast(coords: CityCoordinates) -> WeatherForecast:
    """Simulate fetching a weather forecast from coordinates.

    Args:
        coords: Geocoded city coordinates.

    Returns:
        Deterministic WeatherForecast.
    """
    temp = round(15.0 + (coords.lat * 0.3) % 20, 1)
    humidity = round(40.0 + (coords.lon * 0.5) % 40, 1)
    conditions = ["sunny", "cloudy", "rainy", "partly cloudy"]
    condition = conditions[int(abs(coords.lat + coords.lon)) % len(conditions)]
    print(f"Forecast for {coords.city}: {temp}C, {humidity}% humidity, {condition}")
    return WeatherForecast(
        city=coords.city,
        temperature=temp,
        humidity=humidity,
        condition=condition,
    )


@task
def aggregate_forecasts(forecasts: list[WeatherForecast]) -> ForecastSummary:
    """Aggregate forecasts into a summary.

    Args:
        forecasts: List of city forecasts.

    Returns:
        ForecastSummary with warmest/coldest/average.
    """
    warmest = max(forecasts, key=lambda f: f.temperature)
    coldest = min(forecasts, key=lambda f: f.temperature)
    avg_temp = round(sum(f.temperature for f in forecasts) / len(forecasts), 1)
    return ForecastSummary(
        city_count=len(forecasts),
        warmest=warmest.city,
        coldest=coldest.city,
        avg_temperature=avg_temp,
        forecasts=forecasts,
    )


@task
def format_forecast_table(summary: ForecastSummary) -> str:
    """Format forecast summary as a markdown table.

    Args:
        summary: The forecast summary.

    Returns:
        Markdown string.
    """
    lines = [
        "# Forecast Summary",
        "",
        f"**Cities:** {summary.city_count} | **Warmest:** {summary.warmest} | "
        f"**Coldest:** {summary.coldest} | **Average:** {summary.avg_temperature}C",
        "",
        "| City | Temp (C) | Humidity (%) | Condition |",
        "|---|---|---|---|",
    ]
    for f in summary.forecasts:
        lines.append(f"| {f.city} | {f.temperature} | {f.humidity} | {f.condition} |")
    markdown = "\n".join(lines)
    create_markdown_artifact(key="forecast-summary", markdown=markdown, description="Forecast summary")
    return markdown


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="data_engineering_multi_source_forecast", log_prints=True)
def multi_source_forecast_flow(
    cities: list[str] | None = None,
) -> ForecastSummary:
    """Geocode cities and fetch forecasts using chained .map() calls.

    Args:
        cities: List of city names. Uses defaults if None.

    Returns:
        ForecastSummary.
    """
    if cities is None:
        cities = ["London", "Paris", "Tokyo", "New York", "Sydney"]

    # Chained .map(): geocode then forecast
    coord_futures = geocode_city.map(cities)
    coords = [f.result() for f in coord_futures]

    forecast_futures = fetch_forecast.map(coords)
    forecasts = [f.result() for f in forecast_futures]

    summary = aggregate_forecasts(forecasts)
    format_forecast_table(summary)

    print(f"Forecast complete: {summary.city_count} cities, avg {summary.avg_temperature}C")
    return summary


if __name__ == "__main__":
    load_dotenv()
    multi_source_forecast_flow()
