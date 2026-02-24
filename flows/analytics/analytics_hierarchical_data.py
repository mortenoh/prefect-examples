"""Hierarchical Data Processing.

Tree-structured data processing with path-based hierarchy depth computation,
parent flattening, and derived columns from nested structure.

Airflow equivalent: DHIS2 org unit hierarchy flattening (DAG 058).
Prefect approach:    Simulate nested hierarchy, flatten, compute level stats,
                     identify roots and leaves.
"""

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class ParentRef(BaseModel):
    """Reference to a parent org unit."""

    id: str
    name: str


class RawOrgUnit(BaseModel):
    """A raw nested org unit record."""

    id: str
    name: str
    level: int
    parent: ParentRef | None
    path: str
    created_by: str


class OrgUnit(BaseModel):
    """A flattened organisational unit record."""

    id: str
    name: str
    level: int
    parent_id: str
    parent_name: str
    path: str
    hierarchy_depth: int
    created_by: str


class RootsAndLeaves(BaseModel):
    """Root and leaf nodes in the hierarchy."""

    roots: list[str]
    leaves: list[str]


class HierarchyReport(BaseModel):
    """Summary hierarchy report."""

    total_units: int
    level_distribution: dict[int, int]
    max_depth: int
    root_count: int
    leaf_count: int


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def simulate_org_hierarchy() -> list[RawOrgUnit]:
    """Generate deterministic nested org hierarchy data.

    Returns:
        List of RawOrgUnit.
    """
    data = [
        RawOrgUnit(
            id="OU_001",
            name="National",
            level=1,
            parent=None,
            path="/OU_001",
            created_by="admin",
        ),
        RawOrgUnit(
            id="OU_002",
            name="Region North",
            level=2,
            parent=ParentRef(id="OU_001", name="National"),
            path="/OU_001/OU_002",
            created_by="admin",
        ),
        RawOrgUnit(
            id="OU_003",
            name="Region South",
            level=2,
            parent=ParentRef(id="OU_001", name="National"),
            path="/OU_001/OU_003",
            created_by="admin",
        ),
        RawOrgUnit(
            id="OU_004",
            name="District Alpha",
            level=3,
            parent=ParentRef(id="OU_002", name="Region North"),
            path="/OU_001/OU_002/OU_004",
            created_by="regional_admin",
        ),
        RawOrgUnit(
            id="OU_005",
            name="District Beta",
            level=3,
            parent=ParentRef(id="OU_002", name="Region North"),
            path="/OU_001/OU_002/OU_005",
            created_by="regional_admin",
        ),
        RawOrgUnit(
            id="OU_006",
            name="District Gamma",
            level=3,
            parent=ParentRef(id="OU_003", name="Region South"),
            path="/OU_001/OU_003/OU_006",
            created_by="regional_admin",
        ),
        RawOrgUnit(
            id="OU_007",
            name="Facility One",
            level=4,
            parent=ParentRef(id="OU_004", name="District Alpha"),
            path="/OU_001/OU_002/OU_004/OU_007",
            created_by="district_admin",
        ),
        RawOrgUnit(
            id="OU_008",
            name="Facility Two",
            level=4,
            parent=ParentRef(id="OU_005", name="District Beta"),
            path="/OU_001/OU_002/OU_005/OU_008",
            created_by="district_admin",
        ),
    ]
    print(f"Simulated {len(data)} org units")
    return data


@task
def flatten_hierarchy(raw: list[RawOrgUnit]) -> list[OrgUnit]:
    """Flatten nested hierarchy into OrgUnit records.

    Extracts parent.id and parent.name from RawOrgUnit and computes
    hierarchy depth from path separators.

    Args:
        raw: RawOrgUnit records.

    Returns:
        List of OrgUnit.
    """
    units: list[OrgUnit] = []
    for r in raw:
        parent_id = r.parent.id if r.parent else ""
        parent_name = r.parent.name if r.parent else ""
        # Depth = number of segments in path (split by /, ignoring leading empty)
        depth = len([s for s in r.path.split("/") if s])

        units.append(
            OrgUnit(
                id=r.id,
                name=r.name,
                level=r.level,
                parent_id=parent_id,
                parent_name=parent_name,
                path=r.path,
                hierarchy_depth=depth,
                created_by=r.created_by,
            )
        )
    print(f"Flattened {len(units)} org units")
    return units


@task
def compute_level_distribution(units: list[OrgUnit]) -> dict[int, int]:
    """Compute distribution of units by level.

    Args:
        units: OrgUnit records.

    Returns:
        Dict mapping level to count.
    """
    dist: dict[int, int] = {}
    for u in units:
        dist[u.level] = dist.get(u.level, 0) + 1
    return dist


@task
def find_roots_and_leaves(units: list[OrgUnit]) -> RootsAndLeaves:
    """Identify root and leaf nodes.

    Args:
        units: OrgUnit records.

    Returns:
        RootsAndLeaves.
    """
    parent_ids = {u.parent_id for u in units if u.parent_id}
    child_ids = {u.id for u in units if u.parent_id}

    roots = [u.name for u in units if u.id not in child_ids]
    leaves = [u.name for u in units if u.id not in parent_ids]

    return RootsAndLeaves(roots=roots, leaves=leaves)


@task
def hierarchy_summary(
    units: list[OrgUnit],
    level_dist: dict[int, int],
    roots_leaves: RootsAndLeaves,
) -> HierarchyReport:
    """Build the final hierarchy report.

    Args:
        units: OrgUnit records.
        level_dist: Level distribution.
        roots_leaves: Roots and leaves.

    Returns:
        HierarchyReport.
    """
    max_depth = max(u.hierarchy_depth for u in units) if units else 0
    return HierarchyReport(
        total_units=len(units),
        level_distribution=level_dist,
        max_depth=max_depth,
        root_count=len(roots_leaves.roots),
        leaf_count=len(roots_leaves.leaves),
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="analytics_hierarchical_data", log_prints=True)
def hierarchical_data_flow() -> HierarchyReport:
    """Run the hierarchical data processing pipeline.

    Returns:
        HierarchyReport.
    """
    raw = simulate_org_hierarchy()
    units = flatten_hierarchy(raw)
    level_dist = compute_level_distribution(units)
    roots_leaves = find_roots_and_leaves(units)
    report = hierarchy_summary(units, level_dist, roots_leaves)
    print(f"Hierarchy analysis: {report.total_units} units, max depth {report.max_depth}")
    return report


if __name__ == "__main__":
    hierarchical_data_flow()
