"""WorldPop Population Stats.

Query the WorldPop stats API to get total population for GeoJSON polygons.
Supports both synchronous and asynchronous execution modes.

Airflow equivalent: PythonOperator + HttpHook with polling sensor.
Prefect approach:   httpx tasks with GeoJSON parameters and status polling.
"""

from __future__ import annotations

import logging
import time
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
POLL_INTERVAL_SECONDS = 5
POLL_TIMEOUT_SECONDS = 120

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class PopulationQuery(BaseModel):
    """Parameters for querying population statistics."""

    year: int = Field(default=2020, description="Population year (2000-2020)")
    geojson: dict[str, Any] = Field(description="GeoJSON polygon geometry")
    run_async: bool = Field(default=False, description="Use async API mode with polling")


class PopulationResult(BaseModel):
    """Population query result for a single polygon."""

    total_population: float = Field(description="Estimated total population")
    year: int = Field(description="Population year queried")
    source: str = Field(default="worldpop", description="Data source")


class PopulationSummary(BaseModel):
    """Summary of population query results."""

    results: list[PopulationResult] = Field(description="Individual query results")
    total_queries: int = Field(description="Number of queries executed")
    markdown: str = Field(description="Markdown summary content")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=[2, 5])
def query_population(year: int, geojson: dict[str, Any], run_async: bool = False) -> PopulationResult:
    """Query the WorldPop stats API for total population within a GeoJSON polygon.

    When run_async is True, submits the job and polls for completion.

    Args:
        year: Population year.
        geojson: GeoJSON polygon geometry.
        run_async: Whether to use async API mode.

    Returns:
        PopulationResult with estimated population.
    """
    import json

    params: dict[str, str] = {
        "dataset": "wpgppop",
        "year": str(year),
        "geojson": json.dumps(geojson),
    }
    if run_async:
        params["runasync"] = "true"

    with httpx.Client(timeout=30) as client:
        resp = client.get(WORLDPOP_STATS_URL, params=params)
        resp.raise_for_status()
        body = resp.json()

    if run_async and body.get("status") == "created":
        task_id = body.get("taskid", "")
        print(f"Async task created: {task_id}, polling for result...")
        body = _poll_async_result(task_id)

    data = body.get("data", {})
    total_pop = float(data.get("total_population", 0))
    print(f"Population for year {year}: {total_pop:,.0f}")
    return PopulationResult(total_population=total_pop, year=year)


def _poll_async_result(task_id: str) -> dict[str, Any]:
    """Poll the async task endpoint until completion or timeout.

    Args:
        task_id: WorldPop async task identifier.

    Returns:
        Final API response body.
    """
    url = f"{WORLDPOP_STATS_URL}?taskid={task_id}"
    start = time.monotonic()
    with httpx.Client(timeout=30) as client:
        while time.monotonic() - start < POLL_TIMEOUT_SECONDS:
            resp = client.get(url)
            resp.raise_for_status()
            body = resp.json()
            status = body.get("status", "")
            if status == "finished":
                return body  # type: ignore[no-any-return]
            if status == "error":
                msg = f"Async task {task_id} failed"
                raise RuntimeError(msg)
            time.sleep(POLL_INTERVAL_SECONDS)
    msg = f"Async task {task_id} timed out after {POLL_TIMEOUT_SECONDS}s"
    raise TimeoutError(msg)


@task
def format_results(results: list[PopulationResult]) -> list[dict[str, Any]]:
    """Format population results as a list of dicts for reporting.

    Args:
        results: List of PopulationResult entries.

    Returns:
        List of formatted dictionaries.
    """
    formatted = []
    for r in results:
        formatted.append(
            {
                "year": r.year,
                "total_population": f"{r.total_population:,.0f}",
                "source": r.source,
            }
        )
    return formatted


@task
def build_summary(results: list[PopulationResult]) -> PopulationSummary:
    """Build a markdown summary of population query results.

    Args:
        results: List of PopulationResult entries.

    Returns:
        PopulationSummary with markdown content.
    """
    lines = [
        "## WorldPop Population Statistics",
        "",
        "| Year | Total Population | Source |",
        "|------|-----------------|--------|",
    ]
    for r in results:
        lines.append(f"| {r.year} | {r.total_population:,.0f} | {r.source} |")
    lines.append("")

    if results:
        total = sum(r.total_population for r in results)
        lines.append(f"**Total across queries:** {total:,.0f}")

    markdown = "\n".join(lines)
    return PopulationSummary(
        results=results,
        total_queries=len(results),
        markdown=markdown,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

# Default GeoJSON: small polygon near Addis Ababa, Ethiopia
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


@flow(name="cloud_worldpop_population_stats", log_prints=True)
def worldpop_population_stats_flow(query: PopulationQuery | None = None) -> PopulationSummary:
    """Query WorldPop for total population within a GeoJSON polygon.

    Args:
        query: Optional PopulationQuery with year, geojson, and async flag.

    Returns:
        PopulationSummary with results and markdown.
    """
    if query is None:
        query = PopulationQuery(year=2020, geojson=_DEFAULT_GEOJSON)

    result = query_population(query.year, query.geojson, query.run_async)
    format_results([result])
    summary = build_summary([result])

    create_markdown_artifact(key="worldpop-population-stats", markdown=summary.markdown)

    return summary


if __name__ == "__main__":
    load_dotenv()
    worldpop_population_stats_flow()
