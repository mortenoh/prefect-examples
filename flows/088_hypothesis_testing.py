"""088 -- Hypothesis Testing.

Educational null-hypothesis pattern: align seismic and weather datasets
on date, test for correlation, and interpret the result (expect ~0).

Airflow equivalent: Earthquake-weather correlation / null hypothesis (DAG 093).
Prefect approach:    Simulate seismic + weather data, align on date, test
                     two hypotheses, and produce an educational summary.
"""

import math
import statistics

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class SeismicRecord(BaseModel):
    """A single seismic observation."""

    date: str
    quake_count: int


class WeatherRecord(BaseModel):
    """A single weather observation."""

    date: str
    temperature: float
    pressure: float


class PressureRecord(BaseModel):
    """A weather observation with only pressure (for selective alignment)."""

    date: str
    pressure: float


class DailyObservation(BaseModel):
    """Aligned observation from two datasets."""

    date: str
    metric_a: float
    metric_b: float


class HypothesisResult(BaseModel):
    """Result of a hypothesis test."""

    hypothesis: str
    r_value: float
    interpretation: str
    is_significant: bool


class HypothesisReport(BaseModel):
    """Summary hypothesis testing report."""

    tests: list[HypothesisResult]
    lesson: str


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def simulate_seismic_data(days: int) -> list[SeismicRecord]:
    """Generate deterministic seismic data.

    Args:
        days: Number of days.

    Returns:
        List of SeismicRecord.
    """
    records: list[SeismicRecord] = []
    for d in range(days):
        # Deterministic pseudo-random earthquake count
        x = ((d * 2654435761 + 42) * 1103515245 + 12345) % (2**31)
        count = x % 5  # 0-4 earthquakes per day
        records.append(SeismicRecord(date=f"day_{d:03d}", quake_count=count))
    print(f"Simulated {len(records)} seismic records")
    return records


@task
def simulate_weather_data(days: int) -> list[WeatherRecord]:
    """Generate deterministic weather data.

    Args:
        days: Number of days.

    Returns:
        List of WeatherRecord.
    """
    records: list[WeatherRecord] = []
    for d in range(days):
        # Temperature follows a seasonal pattern, unrelated to earthquakes
        temp = round(15.0 + 10.0 * math.sin(2 * math.pi * d / 365.0), 1)
        pressure = round(1013.0 + 5.0 * math.cos(2 * math.pi * d / 365.0 + 1.0), 1)
        records.append(WeatherRecord(date=f"day_{d:03d}", temperature=temp, pressure=pressure))
    print(f"Simulated {len(records)} weather records")
    return records


@task
def align_datasets(dataset_a: list[BaseModel], dataset_b: list[BaseModel], key: str) -> list[DailyObservation]:
    """Align two datasets by joining on a common key.

    This returns observations where field_a = first numeric field from dataset_a
    and field_b = first numeric field from dataset_b.

    Args:
        dataset_a: First dataset (BaseModel instances).
        dataset_b: Second dataset (BaseModel instances).
        key: Join key field.

    Returns:
        List of aligned DailyObservation.
    """
    b_lookup: dict[str, dict] = {getattr(r, key): r.model_dump() for r in dataset_b}
    aligned: list[DailyObservation] = []
    for a_model in dataset_a:
        a_rec = a_model.model_dump()
        b_rec = b_lookup.get(a_rec[key])
        if b_rec is None:
            continue
        # Get numeric fields (excluding the key)
        a_fields = [k for k in a_rec if k != key and isinstance(a_rec[k], (int, float))]
        b_fields = [k for k in b_rec if k != key and isinstance(b_rec[k], (int, float))]
        if not a_fields or not b_fields:
            continue
        aligned.append(
            DailyObservation(
                date=a_rec[key],
                metric_a=float(a_rec[a_fields[0]]),
                metric_b=float(b_rec[b_fields[0]]),
            )
        )
    print(f"Aligned {len(aligned)} observations")
    return aligned


@task
def check_correlation(
    observations: list[DailyObservation],
    hypothesis: str,
) -> HypothesisResult:
    """Test correlation between metric_a and metric_b.

    Args:
        observations: Aligned observations.
        hypothesis: Description of the hypothesis being tested.

    Returns:
        HypothesisResult.
    """
    if len(observations) < 3:
        return HypothesisResult(
            hypothesis=hypothesis,
            r_value=0.0,
            interpretation="Insufficient data",
            is_significant=False,
        )

    x = [o.metric_a for o in observations]
    y = [o.metric_b for o in observations]
    r_val = _pearson(x, y)
    interpretation = _interpret_result(r_val, len(observations))
    is_significant = abs(r_val) > 0.3  # simplified significance threshold

    return HypothesisResult(
        hypothesis=hypothesis,
        r_value=round(r_val, 4),
        interpretation=interpretation,
        is_significant=is_significant,
    )


@task
def hypothesis_summary(results: list[HypothesisResult]) -> HypothesisReport:
    """Build the final hypothesis report.

    Args:
        results: Hypothesis test results.

    Returns:
        HypothesisReport.
    """
    lesson = (
        "Correlation near zero between unrelated phenomena confirms the null hypothesis. "
        "The absence of correlation is itself a valid and informative finding."
    )
    return HypothesisReport(tests=results, lesson=lesson)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pearson(x: list[float], y: list[float]) -> float:
    """Compute Pearson correlation coefficient."""
    n = len(x)
    if n < 2:
        return 0.0
    mean_x = statistics.mean(x)
    mean_y = statistics.mean(y)
    num = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y, strict=True))
    den_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
    den_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))
    if den_x == 0 or den_y == 0:
        return 0.0
    return num / (den_x * den_y)


def _interpret_result(r_value: float, n: int) -> str:
    """Interpret a correlation result."""
    abs_r = abs(r_value)
    if abs_r < 0.1:
        strength = "negligible"
    elif abs_r < 0.3:
        strength = "weak"
    elif abs_r < 0.5:
        strength = "moderate"
    elif abs_r < 0.7:
        strength = "strong"
    else:
        strength = "very strong"
    direction = "positive" if r_value >= 0 else "negative"
    return f"{strength.capitalize()} {direction} correlation (r={r_value:.4f}, n={n})"


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="088_hypothesis_testing", log_prints=True)
def hypothesis_testing_flow(days: int = 365) -> HypothesisReport:
    """Run the hypothesis testing pipeline.

    Args:
        days: Number of days to simulate.

    Returns:
        HypothesisReport.
    """
    seismic = simulate_seismic_data(days)
    weather = simulate_weather_data(days)

    # Test 1: earthquakes vs temperature
    aligned_temp = align_datasets(seismic, weather, "date")
    result_temp = check_correlation(aligned_temp, "Earthquakes correlate with temperature")

    # Test 2: earthquakes vs pressure
    # Re-align with pressure as metric_b
    weather_pressure = [PressureRecord(date=w.date, pressure=w.pressure) for w in weather]
    aligned_press = align_datasets(seismic, weather_pressure, "date")
    result_press = check_correlation(aligned_press, "Earthquakes correlate with pressure")

    report = hypothesis_summary([result_temp, result_press])
    print(f"Hypothesis testing complete: {len(report.tests)} tests")
    return report


if __name__ == "__main__":
    hypothesis_testing_flow()
