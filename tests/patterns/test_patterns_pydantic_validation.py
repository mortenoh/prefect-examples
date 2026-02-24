"""Tests for flow 047 -- Pydantic Validation."""

import importlib.util
import sys
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "patterns_pydantic_validation",
    Path(__file__).resolve().parent.parent.parent / "flows" / "patterns" / "patterns_pydantic_validation.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["patterns_pydantic_validation"] = _mod
_spec.loader.exec_module(_mod)

WeatherReading = _mod.WeatherReading
ValidationReport = _mod.ValidationReport
generate_readings = _mod.generate_readings
validate_readings = _mod.validate_readings
publish_report = _mod.publish_report
pydantic_validation_flow = _mod.pydantic_validation_flow


def test_valid_weather_reading() -> None:
    reading = WeatherReading(station_id="WX001", temperature=22.5, humidity=65.0)
    assert reading.temperature == 22.5
    assert reading.humidity == 65.0


def test_invalid_temperature() -> None:
    with pytest.raises(ValueError):
        WeatherReading(station_id="WX001", temperature=75.0, humidity=50.0)


def test_invalid_humidity() -> None:
    with pytest.raises(ValueError):
        WeatherReading(station_id="WX001", temperature=20.0, humidity=-5.0)


def test_generate_readings() -> None:
    readings = generate_readings.fn()
    assert isinstance(readings, list)
    assert len(readings) == 5


def test_validate_readings() -> None:
    raw = [
        {"station_id": "WX001", "temperature": 22.5, "humidity": 65.0},
        {"station_id": "WX002", "temperature": 75.0, "humidity": 50.0},
    ]
    report = validate_readings.fn(raw)
    assert isinstance(report, ValidationReport)
    assert report.valid_count == 1
    assert report.invalid_count == 1


def test_publish_report() -> None:
    report = ValidationReport(valid_count=3, invalid_count=1, errors=["error1"])
    result = publish_report.fn(report)
    assert "3 valid" in result
    assert "1 invalid" in result


def test_flow_runs() -> None:
    state = pydantic_validation_flow(return_state=True)
    assert state.is_completed()
