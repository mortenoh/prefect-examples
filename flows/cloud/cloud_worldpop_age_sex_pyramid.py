"""WorldPop Age-Sex Pyramid.

Query the WorldPop age-sex stats API for a GeoJSON polygon, parse the
age-sex pyramid response, and compute demographic summary statistics.

Airflow equivalent: PythonOperator + HttpHook with data transformation.
Prefect approach:   httpx task with structured parsing and markdown artifact.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WORLDPOP_STATS_URL = "https://api.worldpop.org/v1/services/stats"

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class DemographicQuery(BaseModel):
    """Parameters for querying age-sex demographic data."""

    year: int = Field(default=2020, description="Population year (2000-2020)")
    geojson: dict[str, Any] = Field(description="GeoJSON polygon geometry")


class AgeSexBracket(BaseModel):
    """A single age-sex bracket in the pyramid."""

    age_group: str = Field(description="Age group label (e.g. '0-4', '5-9')")
    male: float = Field(description="Male population count")
    female: float = Field(description="Female population count")
    total: float = Field(description="Total population in bracket")


class DemographicStats(BaseModel):
    """Computed demographic statistics from the pyramid."""

    total_population: float = Field(description="Total population")
    total_male: float = Field(description="Total male population")
    total_female: float = Field(description="Total female population")
    sex_ratio: float = Field(description="Males per 100 females")
    dependency_ratio: float = Field(description="(Young + Old) / Working-age * 100")
    median_age_bracket: str = Field(description="Age bracket containing the median person")


class PyramidReport(BaseModel):
    """Full age-sex pyramid report."""

    year: int = Field(description="Population year")
    brackets: list[AgeSexBracket] = Field(description="Age-sex brackets")
    stats: DemographicStats = Field(description="Computed statistics")
    markdown: str = Field(description="Markdown report content")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=[2, 5])
def query_age_sex(year: int, geojson: dict[str, Any]) -> dict[str, Any]:
    """Query the WorldPop age-sex stats API.

    Args:
        year: Population year.
        geojson: GeoJSON polygon geometry.

    Returns:
        Raw API response body as dict.
    """
    import json

    params = {
        "dataset": "wpgpas",
        "year": str(year),
        "geojson": json.dumps(geojson),
    }
    with httpx.Client(timeout=30) as client:
        resp = client.get(WORLDPOP_STATS_URL, params=params)
        resp.raise_for_status()

    body: dict[str, Any] = resp.json()
    print(f"Received age-sex data for year {year}")
    return body


@task
def parse_pyramid(response: dict[str, Any]) -> list[AgeSexBracket]:
    """Parse the API response into structured age-sex brackets.

    The API returns age groups keyed like 'M_0', 'M_1', ..., 'F_0', 'F_1', ...
    where the index maps to 5-year age groups starting at 0-4.

    Args:
        response: Raw API response body.

    Returns:
        List of AgeSexBracket entries.
    """
    data = response.get("data", {})
    agesexpyramid = data.get("agesexpyramid", data)

    age_labels = [
        "0-4",
        "5-9",
        "10-14",
        "15-19",
        "20-24",
        "25-29",
        "30-34",
        "35-39",
        "40-44",
        "45-49",
        "50-54",
        "55-59",
        "60-64",
        "65-69",
        "70-74",
        "75-79",
        "80+",
    ]

    brackets: list[AgeSexBracket] = []
    for i, label in enumerate(age_labels):
        male = float(agesexpyramid.get(f"M_{i}", 0))
        female = float(agesexpyramid.get(f"F_{i}", 0))
        brackets.append(
            AgeSexBracket(
                age_group=label,
                male=male,
                female=female,
                total=male + female,
            )
        )

    total = sum(b.total for b in brackets)
    print(f"Parsed {len(brackets)} age-sex brackets, total population: {total:,.0f}")
    return brackets


@task
def compute_demographics(brackets: list[AgeSexBracket]) -> DemographicStats:
    """Compute summary statistics from age-sex brackets.

    Args:
        brackets: List of AgeSexBracket entries.

    Returns:
        DemographicStats with computed ratios.
    """
    total_male = sum(b.male for b in brackets)
    total_female = sum(b.female for b in brackets)
    total_population = total_male + total_female

    sex_ratio = (total_male / total_female * 100) if total_female > 0 else 0.0

    # Dependency ratio: (0-14 + 65+) / (15-64) * 100
    young = sum(b.total for b in brackets[:3])  # 0-4, 5-9, 10-14
    old = sum(b.total for b in brackets[13:])  # 65+
    working_age = sum(b.total for b in brackets[3:13])  # 15-64
    dependency_ratio = ((young + old) / working_age * 100) if working_age > 0 else 0.0

    # Median age bracket
    cumulative = 0.0
    median_threshold = total_population / 2
    median_bracket = brackets[0].age_group if brackets else "unknown"
    for b in brackets:
        cumulative += b.total
        if cumulative >= median_threshold:
            median_bracket = b.age_group
            break

    stats = DemographicStats(
        total_population=round(total_population, 0),
        total_male=round(total_male, 0),
        total_female=round(total_female, 0),
        sex_ratio=round(sex_ratio, 1),
        dependency_ratio=round(dependency_ratio, 1),
        median_age_bracket=median_bracket,
    )
    print(f"Demographics: sex_ratio={stats.sex_ratio}, dependency_ratio={stats.dependency_ratio}")
    return stats


@task
def build_pyramid_report(year: int, brackets: list[AgeSexBracket], stats: DemographicStats) -> PyramidReport:
    """Build a markdown report with the age-sex pyramid and statistics.

    Args:
        year: Population year.
        brackets: Age-sex brackets.
        stats: Computed statistics.

    Returns:
        PyramidReport with markdown content.
    """
    lines = [
        f"## WorldPop Age-Sex Pyramid ({year})",
        "",
        "### Population by Age Group",
        "",
        "| Age Group | Male | Female | Total |",
        "|-----------|------|--------|-------|",
    ]
    for b in brackets:
        lines.append(f"| {b.age_group} | {b.male:,.0f} | {b.female:,.0f} | {b.total:,.0f} |")

    lines.extend(
        [
            "",
            "### Summary Statistics",
            "",
            f"- **Total population:** {stats.total_population:,.0f}",
            f"- **Male:** {stats.total_male:,.0f}",
            f"- **Female:** {stats.total_female:,.0f}",
            f"- **Sex ratio:** {stats.sex_ratio} males per 100 females",
            f"- **Dependency ratio:** {stats.dependency_ratio}%",
            f"- **Median age bracket:** {stats.median_age_bracket}",
        ]
    )

    markdown = "\n".join(lines)
    return PyramidReport(
        year=year,
        brackets=brackets,
        stats=stats,
        markdown=markdown,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

_DEFAULT_GEOJSON: dict[str, Any] = {
    "type": "Polygon",
    "coordinates": [
        [
            [38.7, 9.0],
            [38.8, 9.0],
            [38.8, 9.1],
            [38.7, 9.1],
            [38.7, 9.0],
        ]
    ],
}


@flow(name="cloud_worldpop_age_sex_pyramid", log_prints=True)
def worldpop_age_sex_pyramid_flow(query: DemographicQuery | None = None) -> PyramidReport:
    """Query WorldPop for age-sex demographics and build a pyramid report.

    Args:
        query: Optional DemographicQuery with year and geojson.

    Returns:
        PyramidReport with brackets, statistics, and markdown.
    """
    if query is None:
        query = DemographicQuery(year=2020, geojson=_DEFAULT_GEOJSON)

    response = query_age_sex(query.year, query.geojson)
    brackets = parse_pyramid(response)
    stats = compute_demographics(brackets)
    report = build_pyramid_report(query.year, brackets, stats)

    create_markdown_artifact(key="worldpop-age-sex-pyramid", markdown=report.markdown)

    return report


if __name__ == "__main__":
    load_dotenv()
    worldpop_age_sex_pyramid_flow()
