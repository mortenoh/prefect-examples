"""102 -- DHIS2 Org Units API.

Block-authenticated metadata fetch with nested JSON flattening and derived
columns (hierarchy depth, translation count).

Airflow equivalent: DHIS2 org unit fetch (DAG 058).
Prefect approach:    Custom block auth, Pydantic flattening, CSV export.
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
RawOrgUnit = _helpers.RawOrgUnit
get_dhis2_connection = _helpers.get_dhis2_connection
get_dhis2_password = _helpers.get_dhis2_password
dhis2_api_fetch = _helpers.dhis2_api_fetch

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class FlatOrgUnit(BaseModel):
    """Flattened org unit with derived columns."""

    id: str
    name: str
    short_name: str
    level: int
    parent_id: str
    created_by: str
    hierarchy_depth: int
    translation_count: int
    opening_date: str


class OrgUnitReport(BaseModel):
    """Summary report for org unit fetch."""

    total: int
    level_distribution: dict[int, int]
    depth_range: tuple[int, int]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def fetch_org_units(conn: Dhis2Connection, password: str) -> list[RawOrgUnit]:
    """Fetch org units from the DHIS2 API.

    Args:
        conn: DHIS2 connection block.
        password: DHIS2 password.

    Returns:
        List of RawOrgUnit.
    """
    raw_dicts = dhis2_api_fetch(conn, "organisationUnits", password)
    units = [RawOrgUnit.model_validate(d) for d in raw_dicts]
    print(f"Fetched {len(units)} org units")
    return units


@task
def flatten_org_units(raw: list[RawOrgUnit]) -> list[FlatOrgUnit]:
    """Flatten raw org units into typed records.

    Extracts parent.id, createdBy.username, computes hierarchy depth from
    path segments, and counts translations.

    Args:
        raw: Raw org unit records.

    Returns:
        List of FlatOrgUnit.
    """
    flat: list[FlatOrgUnit] = []
    for r in raw:
        parent_id = r.parent["id"] if r.parent else ""
        created_by = r.createdBy["username"] if r.createdBy else ""
        depth = len([s for s in r.path.split("/") if s])
        flat.append(
            FlatOrgUnit(
                id=r.id,
                name=r.name,
                short_name=r.shortName,
                level=r.level,
                parent_id=parent_id,
                created_by=created_by,
                hierarchy_depth=depth,
                translation_count=len(r.translations),
                opening_date=r.openingDate,
            )
        )
    print(f"Flattened {len(flat)} org units")
    return flat


@task
def write_org_unit_csv(units: list[FlatOrgUnit], output_dir: str) -> Path:
    """Write flattened org units to CSV.

    Args:
        units: Flattened org units.
        output_dir: Output directory path.

    Returns:
        Path to the CSV file.
    """
    path = Path(output_dir) / "org_units.csv"
    fieldnames = list(FlatOrgUnit.model_fields.keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for u in units:
            writer.writerow(u.model_dump())
    print(f"Wrote {len(units)} org units to {path}")
    return path


@task
def org_unit_report(units: list[FlatOrgUnit]) -> OrgUnitReport:
    """Build a summary report for org units.

    Args:
        units: Flattened org units.

    Returns:
        OrgUnitReport.
    """
    level_dist: dict[int, int] = {}
    depths: list[int] = []
    for u in units:
        level_dist[u.level] = level_dist.get(u.level, 0) + 1
        depths.append(u.hierarchy_depth)
    depth_range = (min(depths), max(depths)) if depths else (0, 0)
    return OrgUnitReport(
        total=len(units),
        level_distribution=level_dist,
        depth_range=depth_range,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="102_dhis2_org_units", log_prints=True)
def dhis2_org_units_flow(output_dir: str | None = None) -> OrgUnitReport:
    """Fetch, flatten, and export DHIS2 org units.

    Args:
        output_dir: Output directory. Uses temp dir if not provided.

    Returns:
        OrgUnitReport.
    """
    if output_dir is None:
        import tempfile

        output_dir = tempfile.mkdtemp(prefix="dhis2_org_units_")

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    conn = get_dhis2_connection()
    password = get_dhis2_password()

    raw = fetch_org_units(conn, password)
    flat = flatten_org_units(raw)
    write_org_unit_csv(flat, output_dir)
    report = org_unit_report(flat)

    create_markdown_artifact(
        key="dhis2-org-unit-report",
        markdown=(
            f"## Org Unit Report\n\n"
            f"- Total: {report.total}\n"
            f"- Depth range: {report.depth_range[0]}--{report.depth_range[1]}\n"
            f"- Levels: {report.level_distribution}\n"
        ),
    )
    print(f"Org unit report: {report.total} units, depth {report.depth_range}")
    return report


if __name__ == "__main__":
    dhis2_org_units_flow()
