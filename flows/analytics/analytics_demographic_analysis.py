"""Demographic Analysis.

Nested JSON flattening into relational/bridge tables, graph edge
construction from border relationships, and country ranking.

Airflow equivalent: Country demographics with nested JSON normalization (DAG 087).
Prefect approach:    Simulate nested country data, flatten to relational tables,
                     build bridge tables and border graph edges, rank countries.
"""

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class RawCountryData(BaseModel):
    """Raw nested country data as received from an API."""

    name: str
    population: int
    area: float
    languages: dict[str, str]
    currencies: dict[str, str]
    borders: list[str]


class Country(BaseModel):
    """Flattened country record."""

    name: str
    population: int
    area: float
    density: float
    languages: dict[str, str]
    currencies: dict[str, str]
    borders: list[str]


class BridgeRecord(BaseModel):
    """Bridge table record linking a country to a key-value attribute."""

    country: str
    key: str
    value: str


class DemographicReport(BaseModel):
    """Summary demographic report."""

    countries: list[Country]
    language_bridge: list[BridgeRecord]
    currency_bridge: list[BridgeRecord]
    border_edges: list[tuple[str, str]]
    rankings: dict[str, list[str]]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def simulate_countries() -> list[RawCountryData]:
    """Generate deterministic nested country data.

    Returns:
        List of RawCountryData.
    """
    data = [
        RawCountryData(
            name="Norway",
            population=5400000,
            area=385207.0,
            languages={"nno": "Norwegian Nynorsk", "nob": "Norwegian Bokmal"},
            currencies={"NOK": "Norwegian krone"},
            borders=["Sweden", "Finland", "Russia"],
        ),
        RawCountryData(
            name="Sweden",
            population=10400000,
            area=450295.0,
            languages={"swe": "Swedish"},
            currencies={"SEK": "Swedish krona"},
            borders=["Norway", "Finland", "Denmark"],
        ),
        RawCountryData(
            name="Finland",
            population=5500000,
            area=338424.0,
            languages={"fin": "Finnish", "swe": "Swedish"},
            currencies={"EUR": "Euro"},
            borders=["Norway", "Sweden", "Russia"],
        ),
        RawCountryData(
            name="Denmark",
            population=5800000,
            area=43094.0,
            languages={"dan": "Danish"},
            currencies={"DKK": "Danish krone"},
            borders=["Sweden"],
        ),
        RawCountryData(
            name="Iceland",
            population=370000,
            area=103000.0,
            languages={"isl": "Icelandic"},
            currencies={"ISK": "Icelandic krona"},
            borders=[],
        ),
    ]
    print(f"Simulated {len(data)} countries")
    return data


@task
def flatten_to_country_table(raw: list[RawCountryData]) -> list[Country]:
    """Flatten nested JSON into Country records.

    Args:
        raw: Raw country data.

    Returns:
        List of Country models.
    """
    countries: list[Country] = []
    for r in raw:
        population = r.population
        area = r.area
        density = round(population / area, 2) if area > 0 else 0.0
        countries.append(
            Country(
                name=r.name,
                population=population,
                area=area,
                density=density,
                languages=r.languages,
                currencies=r.currencies,
                borders=r.borders,
            )
        )
    print(f"Flattened {len(countries)} countries")
    return countries


@task
def build_bridge_table(countries: list[Country], field: str) -> list[BridgeRecord]:
    """Build a bridge table from a multi-valued field.

    Args:
        countries: Country records.
        field: Field name ('languages' or 'currencies').

    Returns:
        List of BridgeRecord.
    """
    records: list[BridgeRecord] = []
    for c in countries:
        mapping = getattr(c, field)
        for key, value in mapping.items():
            records.append(BridgeRecord(country=c.name, key=key, value=value))
    print(f"Built {len(records)} bridge records for {field}")
    return records


@task
def build_border_edges(countries: list[Country]) -> list[tuple[str, str]]:
    """Build directed graph edges from border relationships.

    Args:
        countries: Country records.

    Returns:
        List of (country_a, country_b) tuples.
    """
    edges: list[tuple[str, str]] = []
    for c in countries:
        for border in c.borders:
            edges.append((c.name, border))
    print(f"Built {len(edges)} border edges")
    return edges


@task
def rank_countries(countries: list[Country], metric: str) -> list[str]:
    """Rank countries by a metric (descending).

    Args:
        countries: Country records.
        metric: Field name to rank by.

    Returns:
        List of country names, highest first.
    """
    sorted_countries = sorted(countries, key=lambda c: getattr(c, metric), reverse=True)
    return [c.name for c in sorted_countries]


@task
def demographic_summary(
    countries: list[Country],
    language_bridge: list[BridgeRecord],
    currency_bridge: list[BridgeRecord],
    border_edges: list[tuple[str, str]],
    rankings: dict[str, list[str]],
) -> DemographicReport:
    """Build the final demographic report.

    Args:
        countries: Country records.
        language_bridge: Language bridge table.
        currency_bridge: Currency bridge table.
        border_edges: Border graph edges.
        rankings: Rankings by metric.

    Returns:
        DemographicReport.
    """
    return DemographicReport(
        countries=countries,
        language_bridge=language_bridge,
        currency_bridge=currency_bridge,
        border_edges=border_edges,
        rankings=rankings,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="analytics_demographic_analysis", log_prints=True)
def demographic_analysis_flow() -> DemographicReport:
    """Run the demographic analysis pipeline.

    Returns:
        DemographicReport.
    """
    raw = simulate_countries()
    countries = flatten_to_country_table(raw)

    language_bridge = build_bridge_table(countries, "languages")
    currency_bridge = build_bridge_table(countries, "currencies")
    border_edges = build_border_edges(countries)

    pop_ranking = rank_countries(countries, "population")
    density_ranking = rank_countries(countries, "density")
    rankings = {"population": pop_ranking, "density": density_ranking}

    report = demographic_summary(countries, language_bridge, currency_bridge, border_edges, rankings)
    print(f"Demographic analysis complete: {len(countries)} countries")
    return report


if __name__ == "__main__":
    demographic_analysis_flow()
