"""102 -- DHIS2 Org Units API.

Fetches organisation units from the DHIS2 play server, flattens nested
JSON (parent.id, createdBy.username), computes hierarchy depth from path,
and writes a CSV export.

Airflow equivalent: DHIS2 org unit fetch (DAG 058).
Prefect approach:    Block methods for auth, Pydantic flattening, CSV export.
"""

from __future__ import annotations

import csv
from pathlib import Path

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel

from prefect_examples.dhis2 import (
    Dhis2Connection,
    get_dhis2_connection,
)

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
def fetch_org_units(conn: Dhis2Connection) -> list[dict]:
    """Fetch all organisation units from DHIS2.

    Args:
        conn: DHIS2 connection block.

    Returns:
        List of raw org unit dicts.
    """
    records = conn.fetch_metadata("organisationUnits")
    print(f"Fetched {len(records)} org units")
    return records


@task
def flatten_org_units(raw: list[dict]) -> list[FlatOrgUnit]:
    """Flatten raw org units into typed records.

    Extracts parent.id, createdBy.username, computes hierarchy depth from
    path separators, and counts translations.

    Args:
        raw: Raw org unit dicts from the API.

    Returns:
        List of FlatOrgUnit.
    """
    flat: list[FlatOrgUnit] = []
    for r in raw:
        parent = r.get("parent")
        parent_id = parent["id"] if isinstance(parent, dict) else ""
        created_by_obj = r.get("createdBy")
        created_by = created_by_obj.get("username", "") if isinstance(created_by_obj, dict) else ""
        path = r.get("path", "")
        depth = path.count("/") - 1 if isinstance(path, str) and path else 0
        translations = r.get("translations", [])
        flat.append(
            FlatOrgUnit(
                id=r["id"],
                name=r.get("name", ""),
                short_name=r.get("shortName", ""),
                level=r.get("level", 0),
                parent_id=parent_id,
                created_by=created_by,
                hierarchy_depth=depth,
                translation_count=len(translations) if isinstance(translations, list) else 0,
                opening_date=r.get("openingDate", ""),
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

    raw = fetch_org_units(conn)
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
