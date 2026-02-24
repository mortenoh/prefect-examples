"""Tests for flow 095 -- Spatial Data Construction."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_095",
    Path(__file__).resolve().parent.parent / "flows" / "095_spatial_data.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_095"] = _mod
_spec.loader.exec_module(_mod)

Feature = _mod.Feature
FeatureCollection = _mod.FeatureCollection
SpatialReport = _mod.SpatialReport
simulate_spatial_data = _mod.simulate_spatial_data
build_collection = _mod.build_collection
compute_bounding_box = _mod.compute_bounding_box
filter_by_geometry_type = _mod.filter_by_geometry_type
spatial_summary = _mod.spatial_summary
spatial_data_flow = _mod.spatial_data_flow


def test_simulate_spatial_data() -> None:
    features = simulate_spatial_data.fn()
    assert len(features) == 6
    assert all(isinstance(f, Feature) for f in features)


def test_simulate_returns_features() -> None:
    features = simulate_spatial_data.fn()
    assert features[0].geometry_type == "Point"
    assert features[0].name == "Hospital Alpha"


def test_build_collection() -> None:
    features = simulate_spatial_data.fn()
    collection = build_collection.fn(features)
    assert isinstance(collection, FeatureCollection)
    assert len(collection.features) == 6


def test_bounding_box_correctness() -> None:
    features = simulate_spatial_data.fn()
    bbox = compute_bounding_box.fn(features)
    assert len(bbox) == 4
    assert bbox[0] <= bbox[2]  # min_lon <= max_lon
    assert bbox[1] <= bbox[3]  # min_lat <= max_lat
    # Check specific values from our Point data
    assert bbox[0] == 5.32  # min lon (Clinic Beta)
    assert bbox[2] == 10.75  # max lon (Hospital Alpha)


def test_filter_by_geometry_type_points() -> None:
    features = simulate_spatial_data.fn()
    points = filter_by_geometry_type.fn(features, "Point")
    assert len(points) == 4
    assert all(f.geometry_type == "Point" for f in points)


def test_filter_by_geometry_type_polygons() -> None:
    features = simulate_spatial_data.fn()
    polygons = filter_by_geometry_type.fn(features, "Polygon")
    assert len(polygons) == 2


def test_flow_runs() -> None:
    state = spatial_data_flow(return_state=True)
    assert state.is_completed()
