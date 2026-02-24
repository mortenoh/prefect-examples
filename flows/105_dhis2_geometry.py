"""105 -- DHIS2 Org Unit Geometry API.

Geometry fetch with GeoJSON construction, geometry filtering, and bounding
box computation.

Airflow equivalent: DHIS2 org unit geometry export (DAG 061).
Prefect approach:    Custom block auth, GeoJSON feature collection, bbox.
"""

from __future__ import annotations

import importlib.util
import json
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
simulate_org_units = _helpers.simulate_org_units

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class GeoFeature(BaseModel):
    """A GeoJSON Feature."""

    type: str = "Feature"
    geometry: dict[str, object]
    properties: dict[str, object]


class GeoCollection(BaseModel):
    """A GeoJSON FeatureCollection with bounding box."""

    type: str = "FeatureCollection"
    features: list[GeoFeature]
    bbox: list[float]


class GeometryReport(BaseModel):
    """Summary report for geometry export."""

    feature_count: int
    type_counts: dict[str, int]
    bounding_box: list[float]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def fetch_with_geometry(conn: Dhis2Connection, password: str) -> list[RawOrgUnit]:
    """Fetch org units with geometry from the DHIS2 API.

    Args:
        conn: DHIS2 connection block.
        password: DHIS2 password.

    Returns:
        List of RawOrgUnit (with geometry populated).
    """
    # In a real implementation, request geometry fields:
    # dhis2_api_fetch(conn, "organisationUnits", password,
    #                 fields=["id", "name", "geometry"])
    _ = conn, password
    units = simulate_org_units(20)
    print(f"Fetched {len(units)} org units with geometry")
    return units


@task
def build_features(raw: list[RawOrgUnit]) -> list[GeoFeature]:
    """Build GeoJSON features from org units that have geometry.

    Args:
        raw: Raw org unit records.

    Returns:
        List of GeoFeature (only units with geometry).
    """
    features: list[GeoFeature] = []
    for r in raw:
        if r.geometry is None:
            continue
        features.append(
            GeoFeature(
                geometry=r.geometry,
                properties={
                    "id": r.id,
                    "name": r.name,
                    "level": r.level,
                },
            )
        )
    print(f"Built {len(features)} geo features")
    return features


def _extract_coords(geometry: dict[str, object]) -> list[tuple[float, float]]:
    """Extract all (lon, lat) pairs from a geometry object."""
    geom_type = geometry.get("type", "")
    coords = geometry.get("coordinates", [])
    points: list[tuple[float, float]] = []
    if geom_type == "Point":
        c = coords  # type: ignore[assignment]
        if isinstance(c, list) and len(c) >= 2:
            points.append((float(c[0]), float(c[1])))
    elif geom_type == "Polygon":
        for ring in coords:  # type: ignore[union-attr]
            if isinstance(ring, list):
                for pt in ring:
                    if isinstance(pt, list) and len(pt) >= 2:
                        points.append((float(pt[0]), float(pt[1])))
    return points


@task
def build_collection(features: list[GeoFeature]) -> GeoCollection:
    """Build a GeoJSON FeatureCollection with bounding box.

    Args:
        features: GeoJSON features.

    Returns:
        GeoCollection with computed bbox.
    """
    all_points: list[tuple[float, float]] = []
    for f in features:
        all_points.extend(_extract_coords(f.geometry))

    if all_points:
        lons = [p[0] for p in all_points]
        lats = [p[1] for p in all_points]
        bbox = [min(lons), min(lats), max(lons), max(lats)]
    else:
        bbox = [0.0, 0.0, 0.0, 0.0]

    collection = GeoCollection(features=features, bbox=bbox)
    print(f"Built collection with {len(features)} features, bbox={bbox}")
    return collection


@task
def write_geojson(collection: GeoCollection, output_dir: str) -> Path:
    """Write GeoJSON FeatureCollection to file.

    Args:
        collection: GeoJSON collection.
        output_dir: Output directory path.

    Returns:
        Path to the GeoJSON file.
    """
    path = Path(output_dir) / "org_units.geojson"
    path.write_text(json.dumps(collection.model_dump(), indent=2))
    print(f"Wrote GeoJSON to {path}")
    return path


@task
def geometry_report(collection: GeoCollection) -> GeometryReport:
    """Build a summary report for geometry export.

    Args:
        collection: GeoJSON collection.

    Returns:
        GeometryReport.
    """
    type_counts: dict[str, int] = {}
    for f in collection.features:
        gtype = str(f.geometry.get("type", "unknown"))
        type_counts[gtype] = type_counts.get(gtype, 0) + 1
    return GeometryReport(
        feature_count=len(collection.features),
        type_counts=type_counts,
        bounding_box=collection.bbox,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="105_dhis2_geometry", log_prints=True)
def dhis2_geometry_flow(output_dir: str | None = None) -> GeometryReport:
    """Fetch org unit geometry and build a GeoJSON export.

    Args:
        output_dir: Output directory. Uses temp dir if not provided.

    Returns:
        GeometryReport.
    """
    if output_dir is None:
        import tempfile

        output_dir = tempfile.mkdtemp(prefix="dhis2_geometry_")

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    conn = get_dhis2_connection()
    password = get_dhis2_password()

    raw = fetch_with_geometry(conn, password)
    features = build_features(raw)
    collection = build_collection(features)
    write_geojson(collection, output_dir)
    report = geometry_report(collection)

    create_markdown_artifact(
        key="dhis2-geometry-report",
        markdown=(
            f"## Geometry Report\n\n"
            f"- Features: {report.feature_count}\n"
            f"- Types: {report.type_counts}\n"
            f"- BBox: {report.bounding_box}\n"
        ),
    )
    print(f"Geometry report: {report.feature_count} features")
    return report


if __name__ == "__main__":
    dhis2_geometry_flow()
