"""Daylight Analysis.

Datetime arithmetic for computing day length across latitudes and months.
Computes seasonal profiles and correlates latitude with daylight amplitude.

Airflow equivalent: Sunrise-sunset daylight analysis across latitudes (DAG 085).
Prefect approach:    Simulate monthly sunrise/sunset, compute day lengths,
                     build seasonal profiles, and correlate latitude with amplitude.
"""

import math
import statistics

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class DaylightRecord(BaseModel):
    """Day length data for a city in a specific month."""

    city: str
    latitude: float
    month: int
    sunrise_hour: float
    sunset_hour: float
    day_length_hours: float


class SeasonalProfile(BaseModel):
    """Seasonal daylight profile for a city."""

    city: str
    latitude: float
    min_daylight: float
    max_daylight: float
    amplitude: float
    summer_winter_ratio: float


class DaylightReport(BaseModel):
    """Summary daylight analysis report."""

    profiles: list[SeasonalProfile]
    latitude_correlation: float


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def simulate_daylight(cities_with_lat: list[tuple[str, float]]) -> list[DaylightRecord]:
    """Generate deterministic daylight data based on latitude.

    Uses a simplified formula where day length varies sinusoidally with
    month, with amplitude proportional to latitude.

    Args:
        cities_with_lat: List of (city_name, latitude) tuples.

    Returns:
        List of DaylightRecord (12 per city).
    """
    records: list[DaylightRecord] = []
    for city, lat in cities_with_lat:
        for month in range(1, 13):
            # Day length varies with latitude and month
            # At equator (lat=0), ~12h year round
            # At higher latitudes, more variation
            amplitude = abs(lat) / 90.0 * 6.0  # max +/- 6 hours
            # Peak daylight around month 6 (June) for northern hemisphere
            phase = -0.5 if lat >= 0 else 0.5
            variation = amplitude * math.cos(2 * math.pi * (month - 6) / 12.0 + phase * math.pi)
            day_length = 12.0 + variation
            day_length = max(0.5, min(23.5, day_length))

            sunrise = 12.0 - day_length / 2.0
            sunset = 12.0 + day_length / 2.0

            records.append(
                DaylightRecord(
                    city=city,
                    latitude=lat,
                    month=month,
                    sunrise_hour=round(sunrise, 2),
                    sunset_hour=round(sunset, 2),
                    day_length_hours=round(day_length, 2),
                )
            )
    print(f"Simulated {len(records)} daylight records for {len(cities_with_lat)} cities")
    return records


@task
def compute_seasonal_profiles(records: list[DaylightRecord]) -> list[SeasonalProfile]:
    """Compute seasonal daylight profiles per city.

    Args:
        records: Daylight records.

    Returns:
        List of seasonal profiles.
    """
    by_city: dict[str, list[DaylightRecord]] = {}
    for r in records:
        by_city.setdefault(r.city, []).append(r)

    profiles: list[SeasonalProfile] = []
    for city, city_records in by_city.items():
        day_lengths = [r.day_length_hours for r in city_records]
        min_dl = min(day_lengths)
        max_dl = max(day_lengths)
        amplitude = max_dl - min_dl
        ratio = max_dl / min_dl if min_dl > 0 else 0.0
        lat = city_records[0].latitude

        profiles.append(
            SeasonalProfile(
                city=city,
                latitude=lat,
                min_daylight=round(min_dl, 2),
                max_daylight=round(max_dl, 2),
                amplitude=round(amplitude, 2),
                summer_winter_ratio=round(ratio, 2),
            )
        )
    print(f"Computed {len(profiles)} seasonal profiles")
    return profiles


@task
def correlate_latitude_amplitude(profiles: list[SeasonalProfile]) -> float:
    """Compute Pearson correlation between absolute latitude and amplitude.

    Args:
        profiles: Seasonal profiles.

    Returns:
        Pearson correlation coefficient.
    """
    if len(profiles) < 2:
        return 0.0

    x = [abs(p.latitude) for p in profiles]
    y = [p.amplitude for p in profiles]

    mean_x = statistics.mean(x)
    mean_y = statistics.mean(y)

    numerator = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y, strict=True))
    denom_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
    denom_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))

    if denom_x == 0 or denom_y == 0:
        return 0.0

    r = numerator / (denom_x * denom_y)
    print(f"Latitude-amplitude correlation: {r:.4f}")
    return round(r, 4)


@task
def daylight_summary(profiles: list[SeasonalProfile], correlation: float) -> DaylightReport:
    """Build the final daylight report.

    Args:
        profiles: Seasonal profiles.
        correlation: Latitude-amplitude correlation.

    Returns:
        DaylightReport.
    """
    return DaylightReport(profiles=profiles, latitude_correlation=correlation)


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="analytics_daylight_analysis", log_prints=True)
def daylight_analysis_flow(
    cities_with_lat: list[tuple[str, float]] | None = None,
) -> DaylightReport:
    """Run the daylight analysis pipeline.

    Args:
        cities_with_lat: List of (city, latitude) tuples.

    Returns:
        DaylightReport.
    """
    if cities_with_lat is None:
        cities_with_lat = [
            ("Tromso", 69.6),
            ("Oslo", 59.9),
            ("London", 51.5),
            ("Nairobi", -1.3),
        ]

    records = simulate_daylight(cities_with_lat)
    profiles = compute_seasonal_profiles(records)
    correlation = correlate_latitude_amplitude(profiles)
    report = daylight_summary(profiles, correlation)
    return report


if __name__ == "__main__":
    daylight_analysis_flow()
