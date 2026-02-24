"""103 -- DHIS2 Data Elements API.

Metadata categorization with block auth, boolean derived columns, and
valueType/aggregationType grouping.

Airflow equivalent: DHIS2 data element fetch (DAG 059).
Prefect approach:    Custom block auth, Pydantic flattening, category stats.
"""

from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel

# Import shared helpers
_spec = importlib.util.spec_from_file_location(
    "_dhis2_helpers",
    Path(__file__).resolve().parent / "_dhis2_helpers.py",
)
assert _spec and _spec.loader
_helpers = importlib.util.module_from_spec(_spec)
sys.modules.setdefault("_dhis2_helpers", _helpers)
_spec.loader.exec_module(_helpers)

Dhis2Connection = _helpers.Dhis2Connection
RawDataElement = _helpers.RawDataElement
get_dhis2_connection = _helpers.get_dhis2_connection
get_dhis2_password = _helpers.get_dhis2_password
dhis2_api_fetch = _helpers.dhis2_api_fetch

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
def fetch_data_elements(conn: Dhis2Connection, password: str) -> list[RawDataElement]:
    """Fetch data elements from the DHIS2 API.

    Args:
        conn: DHIS2 connection block.
        password: DHIS2 password.

    Returns:
        List of RawDataElement.
    """
    raw_dicts = dhis2_api_fetch(conn, "dataElements", password)
    elements = [RawDataElement.model_validate(d) for d in raw_dicts]
    print(f"Fetched {len(elements)} data elements")
    return elements


@task
def flatten_data_elements(raw: list[RawDataElement]) -> list[FlatDataElement]:
    """Flatten raw data elements into typed records.

    Args:
        raw: Raw data element records.

    Returns:
        List of FlatDataElement.
    """
    flat: list[FlatDataElement] = []
    for r in raw:
        cc_id = r.categoryCombo["id"] if r.categoryCombo else ""
        flat.append(
            FlatDataElement(
                id=r.id,
                name=r.name,
                short_name=r.shortName,
                domain_type=r.domainType,
                value_type=r.valueType,
                aggregation_type=r.aggregationType,
                category_combo_id=cc_id,
                has_code=r.code is not None,
                name_length=len(r.name),
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


@flow(name="103_dhis2_data_elements", log_prints=True)
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

    conn = get_dhis2_connection()
    password = get_dhis2_password()

    raw = fetch_data_elements(conn, password)
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
