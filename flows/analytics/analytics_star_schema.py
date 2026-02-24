"""Star Schema.

Dimensional modeling with fact and dimension tables. Builds surrogate
keys, applies min-max normalization, and computes a composite index
from weighted normalized indicators.

Airflow equivalent: Health profile dimensional model (DAG 097).
Prefect approach:    Build 3 dimension tables, a fact table, normalize,
                     compute composite index, and rank countries.
"""

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class DimCountry(BaseModel):
    """Country dimension record."""

    key: int
    name: str
    region: str
    population: int


class DimTime(BaseModel):
    """Time dimension record."""

    key: int
    year: int
    decade: int
    is_21st_century: bool


class DimIndicator(BaseModel):
    """Indicator dimension record."""

    key: int
    name: str
    unit: str
    higher_is_better: bool


class FactRecord(BaseModel):
    """Fact table record."""

    country_key: int
    time_key: int
    indicator_key: int
    value: float


class CompositeRanking(BaseModel):
    """Composite ranking for a country."""

    country: str
    composite_score: float


class StarSchemaReport(BaseModel):
    """Summary star schema report."""

    dim_counts: dict[str, int]
    fact_count: int
    composite_rankings: list[CompositeRanking]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def build_country_dimension(data: list[dict]) -> list[DimCountry]:
    """Build country dimension table with surrogate keys.

    Args:
        data: Raw country data.

    Returns:
        List of DimCountry.
    """
    dims: list[DimCountry] = []
    for i, d in enumerate(data, 1):
        dims.append(DimCountry(key=i, name=d["name"], region=d["region"], population=d["population"]))
    print(f"Built country dimension: {len(dims)} records")
    return dims


@task
def build_time_dimension(start_year: int, end_year: int) -> list[DimTime]:
    """Build time dimension table.

    Args:
        start_year: First year.
        end_year: Last year (inclusive).

    Returns:
        List of DimTime.
    """
    dims: list[DimTime] = []
    for i, year in enumerate(range(start_year, end_year + 1), 1):
        dims.append(DimTime(key=i, year=year, decade=(year // 10) * 10, is_21st_century=year >= 2000))
    print(f"Built time dimension: {len(dims)} records")
    return dims


@task
def build_indicator_dimension(indicators: list[dict]) -> list[DimIndicator]:
    """Build indicator dimension table.

    Args:
        indicators: Raw indicator definitions.

    Returns:
        List of DimIndicator.
    """
    dims: list[DimIndicator] = []
    for i, ind in enumerate(indicators, 1):
        dims.append(DimIndicator(key=i, name=ind["name"], unit=ind["unit"], higher_is_better=ind["higher_is_better"]))
    return dims


@task
def build_fact_table(
    countries: list[DimCountry],
    times: list[DimTime],
    indicators: list[DimIndicator],
) -> list[FactRecord]:
    """Build the fact table with deterministic values.

    Args:
        countries: Country dimension.
        times: Time dimension.
        indicators: Indicator dimension.

    Returns:
        List of FactRecord.
    """
    facts: list[FactRecord] = []
    for c in countries:
        for t in times:
            for ind in indicators:
                # Deterministic value based on keys
                base = c.key * 10.0 + t.key * 2.0 + ind.key * 5.0
                value = round(base + (c.key * t.key * ind.key * 7) % 50, 2)
                facts.append(FactRecord(country_key=c.key, time_key=t.key, indicator_key=ind.key, value=value))
    print(f"Built fact table: {len(facts)} records")
    return facts


@task
def min_max_normalize(values: list[float], higher_is_better: bool) -> list[float]:
    """Apply min-max normalization to [0, 1].

    Args:
        values: Input values.
        higher_is_better: If True, higher raw = higher normalized.

    Returns:
        Normalized values in [0, 1].
    """
    if not values:
        return []
    min_v = min(values)
    max_v = max(values)
    if max_v == min_v:
        return [0.5] * len(values)
    normalized = [(v - min_v) / (max_v - min_v) for v in values]
    if not higher_is_better:
        normalized = [1.0 - n for n in normalized]
    return [round(n, 4) for n in normalized]


@task
def compute_composite_index(
    facts: list[FactRecord],
    indicators: list[DimIndicator],
    countries: list[DimCountry],
    times: list[DimTime],
    weights: dict[str, float],
) -> list[CompositeRanking]:
    """Compute composite index from weighted normalized indicators.

    Uses the latest time period for each country.

    Args:
        facts: Fact records.
        indicators: Indicator dimension.
        countries: Country dimension.
        times: Time dimension.
        weights: Weights per indicator name.

    Returns:
        List of CompositeRanking, sorted descending.
    """
    latest_time_key = max(t.key for t in times)
    ind_lookup = {ind.key: ind for ind in indicators}
    # Gather values per indicator for the latest time
    ind_values: dict[int, dict[int, float]] = {}  # indicator_key -> {country_key -> value}
    for f in facts:
        if f.time_key == latest_time_key:
            ind_values.setdefault(f.indicator_key, {})[f.country_key] = f.value

    # Normalize each indicator
    normalized: dict[int, dict[int, float]] = {}
    for ind_key, country_values in ind_values.items():
        ind = ind_lookup[ind_key]
        keys = sorted(country_values.keys())
        raw = [country_values[k] for k in keys]
        normed = min_max_normalize.fn(raw, ind.higher_is_better)
        normalized[ind_key] = dict(zip(keys, normed, strict=True))

    # Compute weighted composite per country
    rankings: list[CompositeRanking] = []
    for c in countries:
        total_weight = 0.0
        weighted_sum = 0.0
        for ind_key, normed_map in normalized.items():
            ind = ind_lookup[ind_key]
            w = weights.get(ind.name, 1.0)
            val = normed_map.get(c.key, 0.0)
            weighted_sum += val * w
            total_weight += w
        composite = weighted_sum / total_weight if total_weight > 0 else 0.0
        rankings.append(CompositeRanking(country=c.name, composite_score=round(composite, 4)))

    rankings.sort(key=lambda x: x.composite_score, reverse=True)
    print(f"Computed composite index for {len(rankings)} countries")
    return rankings


@task
def star_schema_summary(
    dim_counts: dict[str, int],
    fact_count: int,
    composite: list[CompositeRanking],
) -> StarSchemaReport:
    """Build the final star schema report.

    Args:
        dim_counts: Dimension table sizes.
        fact_count: Number of fact records.
        composite: Composite rankings.

    Returns:
        StarSchemaReport.
    """
    return StarSchemaReport(dim_counts=dim_counts, fact_count=fact_count, composite_rankings=composite)


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="analytics_star_schema", log_prints=True)
def star_schema_flow() -> StarSchemaReport:
    """Run the star schema pipeline.

    Returns:
        StarSchemaReport.
    """
    country_data = [
        {"name": "Norway", "region": "Nordic", "population": 5400000},
        {"name": "Sweden", "region": "Nordic", "population": 10400000},
        {"name": "Finland", "region": "Nordic", "population": 5500000},
        {"name": "Denmark", "region": "Nordic", "population": 5800000},
        {"name": "Germany", "region": "Central", "population": 83000000},
    ]
    indicator_defs = [
        {"name": "life_expectancy", "unit": "years", "higher_is_better": True},
        {"name": "infant_mortality", "unit": "per_1000", "higher_is_better": False},
        {"name": "health_spending", "unit": "pct_gdp", "higher_is_better": True},
    ]
    weights = {"life_expectancy": 0.4, "infant_mortality": 0.35, "health_spending": 0.25}

    countries = build_country_dimension(country_data)
    times = build_time_dimension(2018, 2022)
    indicators = build_indicator_dimension(indicator_defs)
    facts = build_fact_table(countries, times, indicators)

    composite = compute_composite_index(facts, indicators, countries, times, weights)

    dim_counts = {"country": len(countries), "time": len(times), "indicator": len(indicators)}
    report = star_schema_summary(dim_counts, len(facts), composite)
    print(f"Star schema complete: {report.fact_count} facts")
    return report


if __name__ == "__main__":
    star_schema_flow()
