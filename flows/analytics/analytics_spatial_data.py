"""Spatial Data Construction.

Manual GeoJSON-like FeatureCollection construction, coordinate extraction,
bounding box computation, and geometry type filtering.

Airflow equivalent: DHIS2 org unit geometry / GeoJSON construction (DAG 061).
Prefect approach:    Simulate spatial data, build features, assemble collection,
                     compute bounding box, and produce summary.
"""

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class Feature(BaseModel):
    """A GeoJSON-like feature."""

    id: str
    name: str
    geometry_type: str
    coordinates: list  # type: ignore[type-arg]
    properties: dict[str, str]


class FeatureCollection(BaseModel):
    """A GeoJSON-like feature collection."""

    type_name: str = "FeatureCollection"
    features: list[Feature]
    bbox: list[float]


class SpatialReport(BaseModel):
    """Summary spatial data report."""

    feature_count: int
    geometry_type_counts: dict[str, int]
    bounding_box: list[float]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def simulate_spatial_data() -> list[Feature]:
    """Generate deterministic spatial data with points and polygons.

    Returns:
        List of Feature models.
    """
    data = [
        Feature(
            id="F001",
            name="Hospital Alpha",
            geometry_type="Point",
            coordinates=[10.75, 59.91],
            properties={"type": "hospital", "level": "tertiary"},
        ),
        Feature(
            id="F002",
            name="Clinic Beta",
            geometry_type="Point",
            coordinates=[5.32, 60.39],
            properties={"type": "clinic", "level": "primary"},
        ),
        Feature(
            id="F003",
            name="District North",
            geometry_type="Polygon",
            coordinates=[[[5.0, 60.0], [11.0, 60.0], [11.0, 62.0], [5.0, 62.0], [5.0, 60.0]]],
            properties={"type": "district", "level": "admin2"},
        ),
        Feature(
            id="F004",
            name="Health Post Gamma",
            geometry_type="Point",
            coordinates=[8.0, 61.5],
            properties={"type": "health_post", "level": "community"},
        ),
        Feature(
            id="F005",
            name="Region West",
            geometry_type="Polygon",
            coordinates=[[[4.0, 58.0], [8.0, 58.0], [8.0, 61.0], [4.0, 61.0], [4.0, 58.0]]],
            properties={"type": "region", "level": "admin1"},
        ),
        Feature(
            id="F006",
            name="Warehouse Delta",
            geometry_type="Point",
            coordinates=[6.5, 58.5],
            properties={"type": "warehouse", "level": "central"},
        ),
    ]
    print(f"Simulated {len(data)} spatial features")
    return data


@task
def build_collection(features: list[Feature]) -> FeatureCollection:
    """Assemble features into a collection with bounding box.

    Args:
        features: List of features.

    Returns:
        FeatureCollection.
    """
    bbox = compute_bounding_box.fn(features)
    return FeatureCollection(features=features, bbox=bbox)


@task
def compute_bounding_box(features: list[Feature]) -> list[float]:
    """Compute bounding box from Point geometries.

    Returns [min_lon, min_lat, max_lon, max_lat].

    Args:
        features: List of features.

    Returns:
        Bounding box as [min_lon, min_lat, max_lon, max_lat].
    """
    lons: list[float] = []
    lats: list[float] = []
    for f in features:
        if f.geometry_type == "Point":
            lons.append(f.coordinates[0])
            lats.append(f.coordinates[1])
    if not lons:
        return [0.0, 0.0, 0.0, 0.0]
    return [min(lons), min(lats), max(lons), max(lats)]


@task
def filter_by_geometry_type(features: list[Feature], geo_type: str) -> list[Feature]:
    """Filter features by geometry type.

    Args:
        features: All features.
        geo_type: Geometry type to filter for.

    Returns:
        Filtered features.
    """
    return [f for f in features if f.geometry_type == geo_type]


@task
def spatial_summary(collection: FeatureCollection) -> SpatialReport:
    """Build the final spatial report.

    Args:
        collection: Feature collection.

    Returns:
        SpatialReport.
    """
    type_counts: dict[str, int] = {}
    for f in collection.features:
        type_counts[f.geometry_type] = type_counts.get(f.geometry_type, 0) + 1

    return SpatialReport(
        feature_count=len(collection.features),
        geometry_type_counts=type_counts,
        bounding_box=collection.bbox,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="analytics_spatial_data", log_prints=True)
def spatial_data_flow() -> SpatialReport:
    """Run the spatial data construction pipeline.

    Returns:
        SpatialReport.
    """
    features = simulate_spatial_data()
    collection = build_collection(features)
    report = spatial_summary(collection)
    print(f"Spatial data: {report.feature_count} features, bbox={report.bounding_box}")
    return report


if __name__ == "__main__":
    spatial_data_flow()
