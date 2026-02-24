"""Tests for flow 069 -- Multi-Source Forecast."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_069",
    Path(__file__).resolve().parent.parent / "flows" / "069_multi_source_forecast.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_069"] = _mod
_spec.loader.exec_module(_mod)

CityCoordinates = _mod.CityCoordinates
WeatherForecast = _mod.WeatherForecast
ForecastSummary = _mod.ForecastSummary
geocode_city = _mod.geocode_city
fetch_forecast = _mod.fetch_forecast
aggregate_forecasts = _mod.aggregate_forecasts
format_forecast_table = _mod.format_forecast_table
multi_source_forecast_flow = _mod.multi_source_forecast_flow


def test_city_coordinates_model() -> None:
    c = CityCoordinates(city="London", lat=51.5, lon=-0.1)
    assert c.city == "London"


def test_geocode_known_city() -> None:
    result = geocode_city.fn("London")
    assert result.lat == 51.5
    assert result.lon == -0.1


def test_geocode_unknown_city() -> None:
    result = geocode_city.fn("Atlantis")
    assert result.lat == 0.0
    assert result.lon == 0.0


def test_fetch_forecast() -> None:
    coords = CityCoordinates(city="London", lat=51.5, lon=-0.1)
    forecast = fetch_forecast.fn(coords)
    assert isinstance(forecast, WeatherForecast)
    assert forecast.city == "London"


def test_aggregate_forecasts() -> None:
    forecasts = [
        WeatherForecast(city="A", temperature=30.0, humidity=50.0, condition="sunny"),
        WeatherForecast(city="B", temperature=10.0, humidity=80.0, condition="rainy"),
    ]
    summary = aggregate_forecasts.fn(forecasts)
    assert summary.warmest == "A"
    assert summary.coldest == "B"
    assert summary.avg_temperature == 20.0


def test_format_forecast_table() -> None:
    forecasts = [WeatherForecast(city="A", temperature=20.0, humidity=50.0, condition="sunny")]
    summary = ForecastSummary(city_count=1, warmest="A", coldest="A", avg_temperature=20.0, forecasts=forecasts)
    md = format_forecast_table.fn(summary)
    assert "| A |" in md


def test_flow_runs() -> None:
    state = multi_source_forecast_flow(return_state=True)
    assert state.is_completed()
