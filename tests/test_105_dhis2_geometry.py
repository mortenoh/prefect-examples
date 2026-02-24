"""Tests for flow 105 -- DHIS2 Org Unit Geometry API."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_105",
    Path(__file__).resolve().parent.parent / "flows" / "105_dhis2_geometry.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_105"] = _mod
_spec.loader.exec_module(_mod)

Dhis2Connection = _mod.Dhis2Connection
RawOrgUnit = _mod.RawOrgUnit
GeoFeature = _mod.GeoFeature
GeoCollection = _mod.GeoCollection
GeometryReport = _mod.GeometryReport
fetch_with_geometry = _mod.fetch_with_geometry
build_features = _mod.build_features
build_collection = _mod.build_collection
write_geojson = _mod.write_geojson
geometry_report = _mod.geometry_report
dhis2_geometry_flow = _mod.dhis2_geometry_flow
_extract_coords = _mod._extract_coords


def test_fetch_with_geometry() -> None:
    conn = Dhis2Connection()
    units = fetch_with_geometry.fn(conn, "district")
    assert len(units) == 20
    with_geom = [u for u in units if u.geometry is not None]
    assert len(with_geom) > 0


def test_build_features() -> None:
    conn = Dhis2Connection()
    raw = fetch_with_geometry.fn(conn, "district")
    features = build_features.fn(raw)
    assert len(features) > 0
    assert all(isinstance(f, GeoFeature) for f in features)
    assert all(f.type == "Feature" for f in features)


def test_feature_properties() -> None:
    conn = Dhis2Connection()
    raw = fetch_with_geometry.fn(conn, "district")
    features = build_features.fn(raw)
    for f in features:
        assert "id" in f.properties
        assert "name" in f.properties
        assert "level" in f.properties


def test_extract_coords_point() -> None:
    coords = _extract_coords({"type": "Point", "coordinates": [32.5, -1.5]})
    assert len(coords) == 1
    assert coords[0] == (32.5, -1.5)


def test_extract_coords_polygon() -> None:
    geom = {"type": "Polygon", "coordinates": [[[32.0, -2.0], [33.0, -2.0], [33.0, -1.0], [32.0, -2.0]]]}
    coords = _extract_coords(geom)
    assert len(coords) == 4


def test_bbox_computation() -> None:
    conn = Dhis2Connection()
    raw = fetch_with_geometry.fn(conn, "district")
    features = build_features.fn(raw)
    collection = build_collection.fn(features)
    assert len(collection.bbox) == 4
    assert collection.bbox[0] <= collection.bbox[2]  # min_lon <= max_lon
    assert collection.bbox[1] <= collection.bbox[3]  # min_lat <= max_lat


def test_geometry_type_counts() -> None:
    conn = Dhis2Connection()
    raw = fetch_with_geometry.fn(conn, "district")
    features = build_features.fn(raw)
    collection = build_collection.fn(features)
    report = geometry_report.fn(collection)
    assert report.feature_count == len(features)
    assert sum(report.type_counts.values()) == report.feature_count


def test_write_geojson(tmp_path: Path) -> None:
    conn = Dhis2Connection()
    raw = fetch_with_geometry.fn(conn, "district")
    features = build_features.fn(raw)
    collection = build_collection.fn(features)
    path = write_geojson.fn(collection, str(tmp_path))
    assert path.exists()
    assert path.name == "org_units.geojson"


def test_flow_runs(tmp_path: Path) -> None:
    state = dhis2_geometry_flow(output_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
