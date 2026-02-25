"""Multi-Indicator Correlation.

Multi-indicator join on (country, year), forward-fill missing values,
Pearson correlation matrix, and year-over-year growth rate computation.

Airflow equivalent: World Bank multi-indicator analysis with correlation (DAG 088).
Prefect approach:    Simulate 3 indicators, join on country+year, forward-fill,
                     compute correlation matrix and growth rates.
"""

import math
import statistics
from typing import Any

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class IndicatorSeries(BaseModel):
    """Time series for one indicator in one country."""

    indicator_name: str
    country: str
    year_values: dict[int, float | None]


class CorrelationResult(BaseModel):
    """Pearson correlation between two indicators."""

    indicator_a: str
    indicator_b: str
    r_value: float
    sample_size: int


class JoinedRecord(BaseModel):
    """A joined record with country, year, and optional indicator values."""

    country: str
    year: int
    GDP: float | None = None
    CO2: float | None = None
    Renewable: float | None = None


class GrowthRates(BaseModel):
    """Average year-over-year growth rates per country for an indicator."""

    rates: dict[str, float]


class IndicatorReport(BaseModel):
    """Summary indicator analysis report."""

    joined_count: int
    correlations: list[CorrelationResult]
    growth_rates: dict[str, GrowthRates]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def simulate_indicator(name: str, countries: list[str], years: list[int]) -> list[IndicatorSeries]:
    """Generate deterministic indicator data.

    Args:
        name: Indicator name.
        countries: Country names.
        years: Year range.

    Returns:
        List of IndicatorSeries.
    """
    series: list[IndicatorSeries] = []
    for ci, country in enumerate(countries):
        values: dict[int, float | None] = {}
        for yi, year in enumerate(years):
            # Simulate some missing values
            if name == "GDP" and yi == 2 and ci == 0:
                values[year] = None
            elif name == "GDP":
                values[year] = round(1000.0 + ci * 500.0 + yi * 50.0, 1)
            elif name == "CO2":
                values[year] = round(5.0 + ci * 2.0 + yi * 0.3, 2)
            else:  # renewable
                values[year] = round(10.0 + ci * 5.0 + yi * 1.5, 2)
        series.append(IndicatorSeries(indicator_name=name, country=country, year_values=values))
    print(f"Simulated {name} for {len(countries)} countries")
    return series


@task
def join_indicators(
    series_list: list[list[IndicatorSeries]],
    countries: list[str],
    years: list[int],
) -> list[JoinedRecord]:
    """Join multiple indicator series on (country, year).

    Args:
        series_list: List of indicator series groups.
        countries: Country names.
        years: Year range.

    Returns:
        List of JoinedRecord with all indicators.
    """
    # Build lookup: (indicator, country) -> {year: value}
    lookup: dict[tuple[str, str], dict[int, float | None]] = {}
    indicators: list[str] = []
    for group in series_list:
        for s in group:
            lookup[(s.indicator_name, s.country)] = s.year_values
            if s.indicator_name not in indicators:
                indicators.append(s.indicator_name)

    records: list[JoinedRecord] = []
    for country in countries:
        for year in years:
            fields: dict[str, Any] = {"country": country, "year": year}
            for ind in indicators:
                val = lookup.get((ind, country), {}).get(year)
                fields[ind] = val
            records.append(JoinedRecord(**fields))
    print(f"Joined {len(records)} records across {len(indicators)} indicators")
    return records


@task
def forward_fill(records: list[JoinedRecord], columns: list[str]) -> list[JoinedRecord]:
    """Forward-fill missing values within each country.

    Args:
        records: Joined records sorted by country and year.
        columns: Columns to forward-fill.

    Returns:
        Records with missing values filled.
    """
    by_country: dict[str, list[JoinedRecord]] = {}
    for r in records:
        by_country.setdefault(r.country, []).append(r)

    filled: list[JoinedRecord] = []
    for country in sorted(by_country.keys()):
        country_records = sorted(by_country[country], key=lambda x: x.year)
        last_values: dict[str, float] = {}
        for r in country_records:
            updates: dict[str, float] = {}
            for col in columns:
                val = getattr(r, col)
                if val is None and col in last_values:
                    updates[col] = last_values[col]
                elif val is not None:
                    last_values[col] = val
            if updates:
                filled.append(r.model_copy(update=updates))
            else:
                filled.append(r)
    return filled


@task
def compute_correlations(records: list[JoinedRecord], indicators: list[str]) -> list[CorrelationResult]:
    """Compute pairwise Pearson correlations between indicators.

    Args:
        records: Joined records.
        indicators: Indicator names.

    Returns:
        List of correlation results.
    """
    results: list[CorrelationResult] = []
    for i, ind_a in enumerate(indicators):
        for ind_b in indicators[i + 1 :]:
            pairs = [
                (getattr(r, ind_a), getattr(r, ind_b))
                for r in records
                if getattr(r, ind_a) is not None and getattr(r, ind_b) is not None
            ]
            if len(pairs) < 3:
                continue
            x_vals = [p[0] for p in pairs]
            y_vals = [p[1] for p in pairs]
            r_val = _pearson(x_vals, y_vals)
            results.append(
                CorrelationResult(
                    indicator_a=ind_a,
                    indicator_b=ind_b,
                    r_value=round(r_val, 4),
                    sample_size=len(pairs),
                )
            )
    return results


@task
def compute_growth_rates(records: list[JoinedRecord], indicator: str) -> GrowthRates:
    """Compute average year-over-year growth rate per country.

    Args:
        records: Joined records.
        indicator: Indicator name.

    Returns:
        GrowthRates with per-country average growth rates.
    """
    by_country: dict[str, list[JoinedRecord]] = {}
    for r in records:
        if getattr(r, indicator) is not None:
            by_country.setdefault(r.country, []).append(r)

    growth: dict[str, float] = {}
    for country, cr in by_country.items():
        cr_sorted = sorted(cr, key=lambda x: x.year)
        rates: list[float] = []
        for j in range(1, len(cr_sorted)):
            prev = getattr(cr_sorted[j - 1], indicator)
            curr = getattr(cr_sorted[j], indicator)
            if prev > 0:
                rates.append((curr - prev) / prev)
        growth[country] = round(statistics.mean(rates), 4) if rates else 0.0
    return GrowthRates(rates=growth)


@task
def indicator_summary(
    joined_count: int,
    correlations: list[CorrelationResult],
    growth_rates: dict[str, GrowthRates],
) -> IndicatorReport:
    """Build the final indicator report.

    Args:
        joined_count: Number of joined records.
        correlations: Correlation results.
        growth_rates: Growth rates by indicator.

    Returns:
        IndicatorReport.
    """
    return IndicatorReport(
        joined_count=joined_count,
        correlations=correlations,
        growth_rates=growth_rates,
    )


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


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="analytics_multi_indicator_correlation", log_prints=True)
def multi_indicator_correlation_flow(
    countries: list[str] | None = None,
    years: list[int] | None = None,
) -> IndicatorReport:
    """Run the multi-indicator correlation pipeline.

    Args:
        countries: Country names.
        years: Year range.

    Returns:
        IndicatorReport.
    """
    if countries is None:
        countries = ["Norway", "Sweden", "Finland"]
    if years is None:
        years = list(range(2010, 2021))

    gdp_series = simulate_indicator("GDP", countries, years)
    co2_series = simulate_indicator("CO2", countries, years)
    ren_series = simulate_indicator("Renewable", countries, years)

    joined = join_indicators([gdp_series, co2_series, ren_series], countries, years)
    filled = forward_fill(joined, ["GDP", "CO2", "Renewable"])

    correlations = compute_correlations(filled, ["GDP", "CO2", "Renewable"])
    gdp_growth = compute_growth_rates(filled, "GDP")
    co2_growth = compute_growth_rates(filled, "CO2")

    report = indicator_summary(
        len(filled),
        correlations,
        {"GDP": gdp_growth, "CO2": co2_growth},
    )
    print(f"Indicator analysis complete: {len(filled)} records, {len(correlations)} correlations")
    return report


if __name__ == "__main__":
    multi_indicator_correlation_flow()
