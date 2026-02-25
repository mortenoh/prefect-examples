"""Regression Analysis.

Simple linear regression (manual OLS), log transformation for skewed
data, R-squared computation, and residual-based efficiency ranking.

Airflow equivalent: Health expenditure log-linear regression (DAG 096).
Prefect approach:    Simulate health data, log-transform spending, regress
                     log(spending) vs mortality, rank by residual.
"""

import math
import statistics

from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class RegressionResult(BaseModel):
    """Result of a linear regression."""

    slope: float
    intercept: float
    r_squared: float
    n: int


class EfficiencyRank(BaseModel):
    """Efficiency ranking based on regression residuals."""

    entity: str
    actual: float
    predicted: float
    residual: float
    rank: int


class RegressionReport(BaseModel):
    """Summary regression analysis report."""

    regression: RegressionResult
    rankings: list[EfficiencyRank]
    top_efficient: list[str]
    bottom_efficient: list[str]


class HealthRecord(BaseModel):
    """A single health expenditure and mortality record."""

    country: str
    spending: float
    mortality: float


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def simulate_health_data(countries: list[str]) -> list[HealthRecord]:
    """Generate deterministic health expenditure and mortality data.

    Args:
        countries: Country names.

    Returns:
        List of HealthRecord.
    """
    data: list[HealthRecord] = []
    for i, country in enumerate(countries):
        # Spending increases, mortality generally decreases with spending
        spending = round(100.0 + i * 200.0 + (i * 37) % 150, 1)
        # Add some noise to create interesting residuals
        noise = ((i * 2654435761) % 100) / 100.0 * 20.0 - 10.0
        mortality = round(max(5.0, 80.0 - math.log(max(1, spending)) * 10.0 + noise), 1)
        data.append(HealthRecord(country=country, spending=spending, mortality=mortality))
    print(f"Simulated health data for {len(countries)} countries")
    return data


@task
def log_transform(values: list[float]) -> list[float]:
    """Apply natural log transformation to values.

    Args:
        values: Input values (must be positive).

    Returns:
        Log-transformed values.
    """
    return [round(math.log(v), 6) for v in values if v > 0]


@task
def linear_regression(x: list[float], y: list[float]) -> RegressionResult:
    """Perform simple OLS linear regression.

    slope = cov(x,y) / var(x)
    intercept = mean(y) - slope * mean(x)
    r_squared = 1 - SS_res / SS_tot

    Args:
        x: Independent variable values.
        y: Dependent variable values.

    Returns:
        RegressionResult.
    """
    n = len(x)
    mean_x = statistics.mean(x)
    mean_y = statistics.mean(y)

    cov_xy = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y, strict=True)) / n
    var_x = sum((xi - mean_x) ** 2 for xi in x) / n

    slope = cov_xy / var_x if var_x > 0 else 0.0
    intercept = mean_y - slope * mean_x

    # R-squared
    ss_res = sum((yi - (slope * xi + intercept)) ** 2 for xi, yi in zip(x, y, strict=True))
    ss_tot = sum((yi - mean_y) ** 2 for yi in y)
    r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

    print(f"Regression: slope={slope:.4f}, intercept={intercept:.4f}, R2={r_squared:.4f}")
    return RegressionResult(
        slope=round(slope, 6),
        intercept=round(intercept, 6),
        r_squared=round(r_squared, 6),
        n=n,
    )


@task
def predict(regression: RegressionResult, x_values: list[float]) -> list[float]:
    """Predict y values from regression.

    Args:
        regression: Regression result.
        x_values: X values to predict for.

    Returns:
        Predicted y values.
    """
    return [round(regression.slope * x + regression.intercept, 4) for x in x_values]


@task
def rank_by_residual(
    actuals: list[float],
    predicted: list[float],
    entities: list[str],
) -> list[EfficiencyRank]:
    """Rank entities by regression residual.

    Negative residual = actual < predicted = more efficient (lower mortality
    than expected for the spending level).

    Args:
        actuals: Actual y values.
        predicted: Predicted y values.
        entities: Entity names.

    Returns:
        List of EfficiencyRank, sorted by residual ascending.
    """
    items: list[EfficiencyRank] = []
    for entity, actual, pred in zip(entities, actuals, predicted, strict=True):
        residual = round(actual - pred, 4)
        items.append(EfficiencyRank(entity=entity, actual=actual, predicted=round(pred, 4), residual=residual, rank=0))
    items.sort(key=lambda x: x.residual)
    for rank_idx, item in enumerate(items, 1):
        item.rank = rank_idx
    return items


@task
def regression_summary(
    regression: RegressionResult,
    rankings: list[EfficiencyRank],
) -> RegressionReport:
    """Build the final regression report.

    Args:
        regression: Regression result.
        rankings: Efficiency rankings.

    Returns:
        RegressionReport.
    """
    top_n = min(3, len(rankings))
    top = [r.entity for r in rankings[:top_n]]
    bottom = [r.entity for r in rankings[-top_n:]]
    return RegressionReport(
        regression=regression,
        rankings=rankings,
        top_efficient=top,
        bottom_efficient=bottom,
    )


@task
def build_regression_artifact(report: RegressionReport) -> str:
    """Build a markdown artifact for the regression report.

    Args:
        report: Regression report.

    Returns:
        Markdown string.
    """
    lines = [
        "# Regression Analysis Report",
        "",
        f"**R-squared:** {report.regression.r_squared:.4f}",
        f"**Slope:** {report.regression.slope:.4f}",
        f"**Sample size:** {report.regression.n}",
        "",
        "## Efficiency Rankings",
        "",
        "| Rank | Entity | Actual | Predicted | Residual |",
        "|---|---|---|---|---|",
    ]
    for r in report.rankings:
        lines.append(f"| {r.rank} | {r.entity} | {r.actual} | {r.predicted} | {r.residual} |")
    markdown = "\n".join(lines)
    create_markdown_artifact(key="regression-report", markdown=markdown, description="Regression Report")
    return markdown


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="analytics_regression_analysis", log_prints=True)
def regression_analysis_flow(
    countries: list[str] | None = None,
) -> RegressionReport:
    """Run the regression analysis pipeline.

    Args:
        countries: Country names.

    Returns:
        RegressionReport.
    """
    if countries is None:
        countries = [
            "Alpha",
            "Beta",
            "Gamma",
            "Delta",
            "Epsilon",
            "Zeta",
            "Eta",
            "Theta",
            "Iota",
            "Kappa",
        ]

    data = simulate_health_data(countries)
    spending = [d.spending for d in data]
    mortality = [d.mortality for d in data]
    entities = [d.country for d in data]

    log_spending = log_transform(spending)
    regression = linear_regression(log_spending, mortality)
    predicted = predict(regression, log_spending)
    rankings = rank_by_residual(mortality, predicted, entities)
    report = regression_summary(regression, rankings)
    build_regression_artifact(report)
    return report


if __name__ == "__main__":
    load_dotenv()
    regression_analysis_flow()
