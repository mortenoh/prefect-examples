"""Tests for flow 105 -- DHIS2 Org Unit Geometry API."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "flow_105",
    Path(__file__).resolve().parent.parent / "flows" / "105_dhis2_geometry.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_105"] = _mod
_spec.loader.exec_module(_mod)

from prefect_examples.dhis2 import Dhis2Client, Dhis2Credentials  # noqa: E402

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

SAMPLE_ORG_UNITS_GEOM = [
    {
        "id": "OU001",
        "name": "National",
        "shortName": "Nat",
        "level": 1,
        "parent": None,
        "geometry": {"type": "Point", "coordinates": [32.5, -1.5]},
    },
    {
        "id": "OU002",
        "name": "Region",
        "shortName": "RN",
        "level": 2,
        "parent": {"id": "OU001"},
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[32.0, -2.0], [33.0, -2.0], [33.0, -1.0], [32.0, -1.0], [32.0, -2.0]]],
        },
    },
    {
        "id": "OU003",
        "name": "NoGeom",
        "shortName": "NG",
        "level": 3,
        "parent": {"id": "OU002"},
    },
]


@patch.object(Dhis2Client, "fetch_metadata")
def test_fetch_with_geometry(mock_fetch: MagicMock) -> None:
    mock_fetch.return_value = SAMPLE_ORG_UNITS_GEOM
    client = MagicMock(spec=Dhis2Client)
    client.fetch_metadata = mock_fetch
    units = fetch_with_geometry.fn(client)
    assert len(units) == 3


def test_build_features() -> None:
    features = build_features.fn(SAMPLE_ORG_UNITS_GEOM)
    assert len(features) == 2  # OU003 has no geometry
    assert all(isinstance(f, GeoFeature) for f in features)
    assert all(f.type == "Feature" for f in features)


def test_feature_properties() -> None:
    features = build_features.fn(SAMPLE_ORG_UNITS_GEOM)
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
    features = build_features.fn(SAMPLE_ORG_UNITS_GEOM)
    collection = build_collection.fn(features)
    assert len(collection.bbox) == 4
    assert collection.bbox[0] <= collection.bbox[2]  # min_lon <= max_lon
    assert collection.bbox[1] <= collection.bbox[3]  # min_lat <= max_lat


def test_geometry_type_counts() -> None:
    features = build_features.fn(SAMPLE_ORG_UNITS_GEOM)
    collection = build_collection.fn(features)
    report = geometry_report.fn(collection)
    assert report.feature_count == 2
    assert report.type_counts.get("Point", 0) == 1
    assert report.type_counts.get("Polygon", 0) == 1


def test_write_geojson(tmp_path: Path) -> None:
    features = build_features.fn(SAMPLE_ORG_UNITS_GEOM)
    collection = build_collection.fn(features)
    path = write_geojson.fn(collection, str(tmp_path))
    assert path.exists()
    assert path.name == "org_units.geojson"


@patch.object(Dhis2Credentials, "get_client")
def test_flow_runs(mock_get_client: MagicMock, tmp_path: Path) -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_ORG_UNITS_GEOM
    mock_get_client.return_value = mock_client
    state = dhis2_geometry_flow(output_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
