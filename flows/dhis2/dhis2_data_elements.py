"""DHIS2 Data Elements API.

Fetches data elements from the DHIS2 play server, flattens
categoryCombo.id, derives boolean has_code and name_length columns,
and groups by valueType/aggregationType.

Airflow equivalent: DHIS2 data element fetch (DAG 059).
Prefect approach:    Block methods for auth, Pydantic flattening, category stats.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel

from prefect_examples.dhis2 import (
    Dhis2Client,
    get_dhis2_credentials,
)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class FlatDataElement(BaseModel):
    """Flattened data element with derived columns."""

    id: str
    name: str
    short_name: str
    domain_type: str
    value_type: str
    aggregation_type: str
    category_combo_id: str
    has_code: bool
    name_length: int


class DataElementReport(BaseModel):
    """Summary report for data elements."""

    total: int
    value_type_counts: dict[str, int]
    aggregation_type_counts: dict[str, int]
    code_coverage: float


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def fetch_data_elements(client: Dhis2Client) -> list[dict[str, Any]]:
    """Fetch all data elements from DHIS2.

    Args:
        client: Authenticated DHIS2 API client.

    Returns:
        List of raw data element dicts.
    """
    records = client.fetch_metadata("dataElements")
    print(f"Fetched {len(records)} data elements")
    return records


@task
def flatten_data_elements(raw: list[dict[str, Any]]) -> list[FlatDataElement]:
    """Flatten raw data elements into typed records.

    Args:
        raw: Raw data element dicts from the API.

    Returns:
        List of FlatDataElement.
    """
    flat: list[FlatDataElement] = []
    for r in raw:
        cc = r.get("categoryCombo")
        cc_id = cc["id"] if isinstance(cc, dict) else ""
        code = r.get("code")
        flat.append(
            FlatDataElement(
                id=r["id"],
                name=r.get("name", ""),
                short_name=r.get("shortName", ""),
                domain_type=r.get("domainType", ""),
                value_type=r.get("valueType", ""),
                aggregation_type=r.get("aggregationType", ""),
                category_combo_id=cc_id,
                has_code=code is not None and code != "",
                name_length=len(r.get("name", "")),
            )
        )
    print(f"Flattened {len(flat)} data elements")
    return flat


@task
def write_data_element_csv(elements: list[FlatDataElement], output_dir: str) -> Path:
    """Write flattened data elements to CSV.

    Args:
        elements: Flattened data elements.
        output_dir: Output directory path.

    Returns:
        Path to the CSV file.
    """
    path = Path(output_dir) / "data_elements.csv"
    fieldnames = list(FlatDataElement.model_fields.keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for e in elements:
            writer.writerow(e.model_dump())
    print(f"Wrote {len(elements)} data elements to {path}")
    return path


@task
def data_element_report(elements: list[FlatDataElement]) -> DataElementReport:
    """Build a summary report for data elements.

    Args:
        elements: Flattened data elements.

    Returns:
        DataElementReport.
    """
    vt_counts: dict[str, int] = {}
    at_counts: dict[str, int] = {}
    coded = 0
    for e in elements:
        vt_counts[e.value_type] = vt_counts.get(e.value_type, 0) + 1
        at_counts[e.aggregation_type] = at_counts.get(e.aggregation_type, 0) + 1
        if e.has_code:
            coded += 1
    coverage = coded / len(elements) if elements else 0.0
    return DataElementReport(
        total=len(elements),
        value_type_counts=vt_counts,
        aggregation_type_counts=at_counts,
        code_coverage=round(coverage, 4),
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="dhis2_data_elements", log_prints=True)
def dhis2_data_elements_flow(output_dir: str | None = None) -> DataElementReport:
    """Fetch, flatten, and export DHIS2 data elements.

    Args:
        output_dir: Output directory. Uses temp dir if not provided.

    Returns:
        DataElementReport.
    """
    if output_dir is None:
        import tempfile

        output_dir = tempfile.mkdtemp(prefix="dhis2_data_elements_")

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    client = get_dhis2_credentials().get_client()

    raw = fetch_data_elements(client)
    flat = flatten_data_elements(raw)
    write_data_element_csv(flat, output_dir)
    report = data_element_report(flat)

    create_markdown_artifact(
        key="dhis2-data-element-report",
        markdown=(
            f"## Data Element Report\n\n"
            f"- Total: {report.total}\n"
            f"- Code coverage: {report.code_coverage:.1%}\n"
            f"- Value types: {report.value_type_counts}\n"
        ),
    )
    print(f"Data element report: {report.total} elements, {report.code_coverage:.1%} coded")
    return report


if __name__ == "__main__":
    dhis2_data_elements_flow()
