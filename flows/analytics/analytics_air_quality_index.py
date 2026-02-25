"""Air Quality Index.

Threshold-based classification of air pollutant readings against WHO
air quality standards. Generates health advisories based on severity.

Airflow equivalent: Air quality monitoring with WHO thresholds (DAG 083).
Prefect approach:    Simulate readings, classify each city, count threshold
                     exceedances, and generate advisories with a markdown artifact.
"""

from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class PollutantReading(BaseModel):
    """A single pollutant measurement for a city."""

    city: str
    pollutant: str
    hourly_values: list[float]


class AqiClassification(BaseModel):
    """AQI classification result for a city/pollutant pair."""

    city: str
    pollutant: str
    mean_value: float
    category: str
    color: str
    health_advisory: str


class AirQualityReport(BaseModel):
    """Summary report across all cities."""

    city_results: list[AqiClassification]
    worst_city: str
    exceedance_count: int
    total_readings: int


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WHO_LIMITS: dict[str, float] = {
    "PM2.5": 15.0,
    "PM10": 45.0,
    "NO2": 25.0,
    "O3": 100.0,
}

AQI_THRESHOLDS: list[tuple[float, str, str]] = [
    (50.0, "Good", "green"),
    (100.0, "Moderate", "yellow"),
    (150.0, "Unhealthy for Sensitive Groups", "orange"),
    (200.0, "Unhealthy", "red"),
    (300.0, "Very Unhealthy", "purple"),
    (float("inf"), "Hazardous", "maroon"),
]

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def simulate_air_quality(cities: list[str]) -> list[PollutantReading]:
    """Generate deterministic air quality readings for cities.

    Args:
        cities: List of city names.

    Returns:
        List of pollutant readings.
    """
    pollutants = ["PM2.5", "PM10", "NO2", "O3"]
    readings: list[PollutantReading] = []
    for ci, city in enumerate(cities):
        for pi, pollutant in enumerate(pollutants):
            base = 10.0 + ci * 15.0 + pi * 8.0
            hourly = [round(base + (h * 1.5 + ci * 3.0 + pi * 2.0) % 20, 1) for h in range(24)]
            readings.append(PollutantReading(city=city, pollutant=pollutant, hourly_values=hourly))
    print(f"Simulated {len(readings)} readings for {len(cities)} cities")
    return readings


@task
def classify_aqi(readings: list[PollutantReading]) -> list[AqiClassification]:
    """Classify each reading based on AQI thresholds.

    Args:
        readings: Pollutant readings.

    Returns:
        List of AQI classifications.
    """
    classifications: list[AqiClassification] = []
    for reading in readings:
        mean_val = sum(reading.hourly_values) / len(reading.hourly_values)
        category = "Good"
        color = "green"
        for threshold, cat, col in AQI_THRESHOLDS:
            if mean_val <= threshold:
                category = cat
                color = col
                break
        advisory = _generate_advisory(category)
        classifications.append(
            AqiClassification(
                city=reading.city,
                pollutant=reading.pollutant,
                mean_value=round(mean_val, 1),
                category=category,
                color=color,
                health_advisory=advisory,
            )
        )
    print(f"Classified {len(classifications)} readings")
    return classifications


@task
def count_exceedances(readings: list[PollutantReading], who_limits: dict[str, float]) -> dict[str, int]:
    """Count how many readings exceed WHO limits.

    Args:
        readings: Pollutant readings.
        who_limits: WHO limit per pollutant.

    Returns:
        Dict mapping pollutant to exceedance count.
    """
    counts: dict[str, int] = {}
    for reading in readings:
        limit = who_limits.get(reading.pollutant)
        if limit is None:
            continue
        mean_val = sum(reading.hourly_values) / len(reading.hourly_values)
        if mean_val > limit:
            counts[reading.pollutant] = counts.get(reading.pollutant, 0) + 1
    return counts


@task
def air_quality_summary(
    classifications: list[AqiClassification],
    exceedances: dict[str, int],
    total_readings: int,
) -> AirQualityReport:
    """Build the final air quality report.

    Args:
        classifications: AQI classifications.
        exceedances: Exceedance counts by pollutant.
        total_readings: Total number of readings processed.

    Returns:
        AirQualityReport.
    """
    severity_order = ["Hazardous", "Very Unhealthy", "Unhealthy", "Unhealthy for Sensitive Groups", "Moderate", "Good"]
    worst_city = ""
    worst_severity = len(severity_order)
    for c in classifications:
        idx = severity_order.index(c.category) if c.category in severity_order else len(severity_order)
        if idx < worst_severity:
            worst_severity = idx
            worst_city = c.city

    total_exc = sum(exceedances.values())
    report = AirQualityReport(
        city_results=classifications,
        worst_city=worst_city,
        exceedance_count=total_exc,
        total_readings=total_readings,
    )
    print(f"Worst city: {worst_city}, exceedances: {total_exc}")
    return report


@task
def build_aqi_artifact(report: AirQualityReport) -> str:
    """Build a markdown artifact for the AQI report.

    Args:
        report: Air quality report.

    Returns:
        Markdown string.
    """
    lines = [
        "# Air Quality Report",
        "",
        f"**Worst city:** {report.worst_city}",
        f"**Exceedances:** {report.exceedance_count}",
        f"**Total readings:** {report.total_readings}",
        "",
        "| City | Pollutant | Mean | Category | Advisory |",
        "|---|---|---|---|---|",
    ]
    for c in report.city_results:
        lines.append(f"| {c.city} | {c.pollutant} | {c.mean_value} | {c.category} | {c.health_advisory} |")
    markdown = "\n".join(lines)
    create_markdown_artifact(key="aqi-report", markdown=markdown, description="Air Quality Report")
    return markdown


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _generate_advisory(category: str) -> str:
    """Generate a health advisory based on AQI category."""
    advisories = {
        "Good": "Air quality is satisfactory.",
        "Moderate": "Unusually sensitive people should consider reducing outdoor exertion.",
        "Unhealthy for Sensitive Groups": "Sensitive groups should reduce outdoor exertion.",
        "Unhealthy": "Everyone should reduce prolonged outdoor exertion.",
        "Very Unhealthy": "Everyone should avoid prolonged outdoor exertion.",
        "Hazardous": "Everyone should avoid all outdoor exertion.",
    }
    return advisories.get(category, "No advisory available.")


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="analytics_air_quality_index", log_prints=True)
def air_quality_index_flow(
    cities: list[str] | None = None,
) -> AirQualityReport:
    """Run the air quality index pipeline.

    Args:
        cities: List of city names. Defaults to sample cities.

    Returns:
        AirQualityReport.
    """
    if cities is None:
        cities = ["Oslo", "Bergen", "Trondheim", "Stavanger"]

    readings = simulate_air_quality(cities)
    classifications = classify_aqi(readings)
    exceedances = count_exceedances(readings, WHO_LIMITS)
    report = air_quality_summary(classifications, exceedances, len(readings))
    build_aqi_artifact(report)
    return report


if __name__ == "__main__":
    load_dotenv()
    air_quality_index_flow()
