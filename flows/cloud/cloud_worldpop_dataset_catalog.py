"""WorldPop Dataset Catalog Explorer.

Browse the WorldPop catalog API: list top-level datasets, drill into
sub-datasets, and query by country ISO3 code.

Airflow equivalent: PythonOperator + HttpHook for REST API traversal.
Prefect approach:   httpx tasks with Pydantic models and markdown artifact.
"""

from __future__ import annotations

import logging

import httpx
from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WORLDPOP_CATALOG_URL = "https://hub.worldpop.org/rest/data"

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class DatasetQuery(BaseModel):
    """Parameters for browsing the WorldPop catalog."""

    dataset: str | None = Field(default=None, description="Top-level dataset ID (e.g. 'pop')")
    subdataset: str | None = Field(default=None, description="Sub-dataset ID (e.g. 'wpgp')")
    iso3: str | None = Field(default=None, description="Country ISO3 code (e.g. 'ETH')")


class DatasetRecord(BaseModel):
    """A single dataset entry from the catalog API."""

    id: str = Field(description="Dataset identifier")
    title: str = Field(default="", description="Human-readable title")
    description: str = Field(default="", description="Dataset description")


class SubDatasetRecord(BaseModel):
    """A sub-dataset entry within a top-level dataset."""

    id: str = Field(description="Sub-dataset identifier")
    title: str = Field(default="", description="Human-readable title")
    description: str = Field(default="", description="Sub-dataset description")


class CountryDatasetRecord(BaseModel):
    """A country-specific dataset record."""

    title: str = Field(default="", description="Dataset title")
    iso3: str = Field(default="", description="Country ISO3 code")
    popyear: str = Field(default="", description="Population year")
    doi: str = Field(default="", description="DOI reference")
    url_summary: str = Field(default="", description="Summary page URL")


class CatalogReport(BaseModel):
    """Final catalog exploration report."""

    datasets_found: int = Field(description="Number of top-level datasets found")
    subdatasets_found: int = Field(description="Number of sub-datasets found")
    country_records_found: int = Field(description="Number of country records found")
    markdown: str = Field(description="Markdown report content")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=[2, 5])
def list_datasets() -> list[DatasetRecord]:
    """Fetch all top-level datasets from the WorldPop catalog.

    Returns:
        List of DatasetRecord entries.
    """
    with httpx.Client(timeout=30) as client:
        resp = client.get(WORLDPOP_CATALOG_URL)
        resp.raise_for_status()

    body = resp.json()
    items = body.get("data", [])
    records = [
        DatasetRecord(
            id=item.get("alias", item.get("id", "")),
            title=item.get("title", ""),
            description=item.get("desc", item.get("description", "")),
        )
        for item in items
    ]
    print(f"Found {len(records)} top-level datasets")
    return records


@task(retries=2, retry_delay_seconds=[2, 5])
def list_subdatasets(dataset: str) -> list[SubDatasetRecord]:
    """Fetch sub-datasets within a top-level dataset.

    Args:
        dataset: Top-level dataset ID (e.g. 'pop').

    Returns:
        List of SubDatasetRecord entries.
    """
    url = f"{WORLDPOP_CATALOG_URL}/{dataset}"
    with httpx.Client(timeout=30) as client:
        resp = client.get(url)
        resp.raise_for_status()

    body = resp.json()
    items = body.get("data", [])
    records = [
        SubDatasetRecord(
            id=item.get("alias", item.get("id", "")),
            title=item.get("title", ""),
            description=item.get("desc", item.get("description", "")),
        )
        for item in items
    ]
    print(f"Found {len(records)} sub-datasets under '{dataset}'")
    return records


@task(retries=2, retry_delay_seconds=[2, 5])
def query_country_datasets(dataset: str, subdataset: str, iso3: str) -> list[CountryDatasetRecord]:
    """Query datasets for a specific country.

    Args:
        dataset: Top-level dataset ID.
        subdataset: Sub-dataset ID.
        iso3: Country ISO3 code.

    Returns:
        List of CountryDatasetRecord entries.
    """
    url = f"{WORLDPOP_CATALOG_URL}/{dataset}/{subdataset}"
    params = {"iso3": iso3}
    with httpx.Client(timeout=30) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()

    body = resp.json()
    items = body.get("data", [])
    records = [
        CountryDatasetRecord(
            title=item.get("title", ""),
            iso3=item.get("iso3", iso3),
            popyear=str(item.get("popyear", "")),
            doi=item.get("doi", ""),
            url_summary=item.get("url_summary", ""),
        )
        for item in items
    ]
    print(f"Found {len(records)} records for {iso3} in {dataset}/{subdataset}")
    return records


@task
def build_catalog_report(
    datasets: list[DatasetRecord],
    subdatasets: list[SubDatasetRecord],
    country_records: list[CountryDatasetRecord],
    query: DatasetQuery,
) -> CatalogReport:
    """Build a markdown report summarising catalog exploration results.

    Args:
        datasets: Top-level datasets.
        subdatasets: Sub-datasets.
        country_records: Country-specific records.
        query: Original query parameters.

    Returns:
        CatalogReport with markdown content.
    """
    lines = ["## WorldPop Dataset Catalog", ""]

    lines.append("### Top-Level Datasets")
    lines.append("")
    if datasets:
        lines.append("| ID | Title |")
        lines.append("|-----|-------|")
        for ds in datasets:
            lines.append(f"| {ds.id} | {ds.title} |")
    else:
        lines.append("No datasets found.")
    lines.append("")

    if query.dataset:
        lines.append(f"### Sub-Datasets under `{query.dataset}`")
        lines.append("")
        if subdatasets:
            lines.append("| ID | Title |")
            lines.append("|-----|-------|")
            for sd in subdatasets:
                lines.append(f"| {sd.id} | {sd.title} |")
        else:
            lines.append("No sub-datasets found.")
        lines.append("")

    if query.iso3 and query.dataset and query.subdataset:
        lines.append(f"### Records for `{query.iso3}` ({query.dataset}/{query.subdataset})")
        lines.append("")
        if country_records:
            lines.append("| Title | Year | DOI |")
            lines.append("|-------|------|-----|")
            for cr in country_records:
                lines.append(f"| {cr.title} | {cr.popyear} | {cr.doi} |")
        else:
            lines.append("No country records found.")
        lines.append("")

    markdown = "\n".join(lines)
    return CatalogReport(
        datasets_found=len(datasets),
        subdatasets_found=len(subdatasets),
        country_records_found=len(country_records),
        markdown=markdown,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="cloud_worldpop_dataset_catalog", log_prints=True)
def worldpop_dataset_catalog_flow(query: DatasetQuery | None = None) -> CatalogReport:
    """Browse the WorldPop catalog: datasets, sub-datasets, and country records.

    Args:
        query: Optional DatasetQuery with dataset, subdataset, and iso3 filters.

    Returns:
        CatalogReport with markdown summary.
    """
    if query is None:
        query = DatasetQuery(dataset="pop", subdataset="wpgp", iso3="ETH")

    datasets = list_datasets()

    subdatasets: list[SubDatasetRecord] = []
    if query.dataset:
        subdatasets = list_subdatasets(query.dataset)

    country_records: list[CountryDatasetRecord] = []
    if query.dataset and query.subdataset and query.iso3:
        country_records = query_country_datasets(query.dataset, query.subdataset, query.iso3)

    report = build_catalog_report(datasets, subdatasets, country_records, query)

    create_markdown_artifact(key="worldpop-dataset-catalog", markdown=report.markdown)

    return report


if __name__ == "__main__":
    load_dotenv()
    worldpop_dataset_catalog_flow()
