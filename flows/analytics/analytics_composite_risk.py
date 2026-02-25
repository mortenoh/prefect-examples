"""Composite Risk Assessment.

Multi-source weighted risk scoring combining marine and flood data.
Normalizes risk factors to 0-100 scale, computes weighted composite,
and classifies into risk categories.

Airflow equivalent: Marine forecast + flood discharge composite risk (DAG 084).
Prefect approach:    Simulate marine + flood data, normalize, compute weighted
                     composite (60/40), classify, and produce a markdown report.
"""

from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class RiskFactor(BaseModel):
    """A single risk factor measurement."""

    source: str
    location: str
    raw_value: float
    normalized_score: float = 0.0


class CompositeRisk(BaseModel):
    """Composite risk result for a location."""

    location: str
    factors: list[RiskFactor]
    weighted_score: float
    category: str


class RiskReport(BaseModel):
    """Summary risk report across all locations."""

    locations: list[CompositeRisk]
    highest_risk: str
    category_counts: dict[str, int]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def simulate_marine_data(locations: list[str]) -> list[RiskFactor]:
    """Generate deterministic marine risk factors.

    Args:
        locations: Location names.

    Returns:
        List of marine risk factors (wave height, swell period).
    """
    factors: list[RiskFactor] = []
    for i, loc in enumerate(locations):
        wave_height = round(1.0 + i * 0.8, 2)
        factors.append(RiskFactor(source="marine_wave", location=loc, raw_value=wave_height))
        swell_period = round(6.0 + i * 1.5, 2)
        factors.append(RiskFactor(source="marine_swell", location=loc, raw_value=swell_period))
    print(f"Simulated {len(factors)} marine risk factors")
    return factors


@task
def simulate_flood_data(locations: list[str]) -> list[RiskFactor]:
    """Generate deterministic flood risk factors.

    Args:
        locations: Location names.

    Returns:
        List of flood risk factors (discharge ratio).
    """
    factors: list[RiskFactor] = []
    for i, loc in enumerate(locations):
        discharge_ratio = round(0.5 + i * 0.4, 2)
        factors.append(RiskFactor(source="flood_discharge", location=loc, raw_value=discharge_ratio))
    print(f"Simulated {len(factors)} flood risk factors")
    return factors


@task
def normalize_risk(factors: list[RiskFactor], thresholds: dict[str, tuple[float, float]]) -> list[RiskFactor]:
    """Normalize risk factor values to 0-100 scale.

    Args:
        factors: Raw risk factors.
        thresholds: Min/max thresholds per source for normalization.

    Returns:
        Risk factors with normalized_score set.
    """
    normalized: list[RiskFactor] = []
    for f in factors:
        lo, hi = thresholds.get(f.source, (0.0, 10.0))
        score = 50.0 if hi == lo else max(0.0, min(100.0, (f.raw_value - lo) / (hi - lo) * 100.0))
        normalized.append(f.model_copy(update={"normalized_score": round(score, 1)}))
    return normalized


@task
def compute_composite(
    location: str,
    marine_factors: list[RiskFactor],
    flood_factors: list[RiskFactor],
    marine_weight: float,
    flood_weight: float,
) -> CompositeRisk:
    """Compute weighted composite risk for a location.

    Args:
        location: Location name.
        marine_factors: Normalized marine factors for this location.
        flood_factors: Normalized flood factors for this location.
        marine_weight: Weight for marine factors.
        flood_weight: Weight for flood factors.

    Returns:
        CompositeRisk.
    """
    all_factors = marine_factors + flood_factors
    marine_scores = [f.normalized_score for f in marine_factors]
    flood_scores = [f.normalized_score for f in flood_factors]

    marine_avg = sum(marine_scores) / len(marine_scores) if marine_scores else 0.0
    flood_avg = sum(flood_scores) / len(flood_scores) if flood_scores else 0.0

    weighted = marine_avg * marine_weight + flood_avg * flood_weight
    category = _classify_risk(weighted)

    return CompositeRisk(
        location=location,
        factors=all_factors,
        weighted_score=round(weighted, 1),
        category=category,
    )


@task
def risk_summary(composites: list[CompositeRisk]) -> RiskReport:
    """Build the final risk report.

    Args:
        composites: Composite risk results per location.

    Returns:
        RiskReport.
    """
    highest = max(composites, key=lambda c: c.weighted_score)
    counts: dict[str, int] = {}
    for c in composites:
        counts[c.category] = counts.get(c.category, 0) + 1

    report = RiskReport(locations=composites, highest_risk=highest.location, category_counts=counts)
    print(f"Highest risk: {highest.location} ({highest.weighted_score})")
    return report


@task
def build_risk_artifact(report: RiskReport) -> str:
    """Build a markdown artifact for the risk report.

    Args:
        report: Risk report.

    Returns:
        Markdown string.
    """
    lines = [
        "# Composite Risk Report",
        "",
        f"**Highest risk:** {report.highest_risk}",
        "",
        "| Location | Score | Category |",
        "|---|---|---|",
    ]
    for loc in report.locations:
        lines.append(f"| {loc.location} | {loc.weighted_score} | {loc.category} |")
    markdown = "\n".join(lines)
    create_markdown_artifact(key="risk-report", markdown=markdown, description="Composite Risk Report")
    return markdown


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _classify_risk(score: float) -> str:
    """Classify a composite risk score into a category."""
    if score < 25.0:
        return "low"
    if score < 50.0:
        return "moderate"
    if score < 75.0:
        return "high"
    return "critical"


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

RISK_THRESHOLDS: dict[str, tuple[float, float]] = {
    "marine_wave": (0.5, 5.0),
    "marine_swell": (4.0, 14.0),
    "flood_discharge": (0.3, 2.5),
}


@flow(name="analytics_composite_risk", log_prints=True)
def composite_risk_flow(
    locations: list[str] | None = None,
    marine_weight: float = 0.6,
    flood_weight: float = 0.4,
) -> RiskReport:
    """Run the composite risk assessment pipeline.

    Args:
        locations: Location names. Defaults to sample locations.
        marine_weight: Weight for marine risk (default 0.6).
        flood_weight: Weight for flood risk (default 0.4).

    Returns:
        RiskReport.
    """
    if locations is None:
        locations = ["Coastal-A", "Coastal-B", "Coastal-C", "Inland-D"]

    marine_raw = simulate_marine_data(locations)
    flood_raw = simulate_flood_data(locations)

    marine_norm = normalize_risk(marine_raw, RISK_THRESHOLDS)
    flood_norm = normalize_risk(flood_raw, RISK_THRESHOLDS)

    composites: list[CompositeRisk] = []
    for loc in locations:
        m_factors = [f for f in marine_norm if f.location == loc]
        f_factors = [f for f in flood_norm if f.location == loc]
        comp = compute_composite(loc, m_factors, f_factors, marine_weight, flood_weight)
        composites.append(comp)

    report = risk_summary(composites)
    build_risk_artifact(report)
    return report


if __name__ == "__main__":
    load_dotenv()
    composite_risk_flow()
