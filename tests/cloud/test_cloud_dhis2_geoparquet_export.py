"""Tests for cloud DHIS2 GeoParquet export flow."""

import importlib.util
import io
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import geopandas as gpd

_spec = importlib.util.spec_from_file_location(
    "cloud_dhis2_geoparquet_export",
    Path(__file__).resolve().parent.parent.parent / "flows" / "cloud" / "cloud_dhis2_geoparquet_export.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["cloud_dhis2_geoparquet_export"] = _mod
_spec.loader.exec_module(_mod)

StorageBackend = _mod.StorageBackend
ExportReport = _mod.ExportReport
fetch_org_units_with_geometry = _mod.fetch_org_units_with_geometry
build_geodataframe = _mod.build_geodataframe
export_geoparquet = _mod.export_geoparquet
upload_to_s3 = _mod.upload_to_s3
build_report = _mod.build_report
dhis2_geoparquet_export_flow = _mod.dhis2_geoparquet_export_flow


# ---------------------------------------------------------------------------
# Sample DHIS2 org unit data (Points + Polygons)
# ---------------------------------------------------------------------------

SAMPLE_ORG_UNITS: list[dict[str, object]] = [
    {
        "id": "ou1",
        "name": "Facility A",
        "shortName": "FacA",
        "level": 4,
        "parent": {"id": "parent1"},
        "geometry": {"type": "Point", "coordinates": [32.5, -1.5]},
    },
    {
        "id": "ou2",
        "name": "Facility B",
        "shortName": "FacB",
        "level": 4,
        "parent": {"id": "parent1"},
        "geometry": {"type": "Point", "coordinates": [33.0, -2.0]},
    },
    {
        "id": "ou3",
        "name": "District C",
        "shortName": "DistC",
        "level": 3,
        "parent": {"id": "parent2"},
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[32.0, -1.0], [33.0, -1.0], [33.0, -2.0], [32.0, -2.0], [32.0, -1.0]]],
        },
    },
]


# ---------------------------------------------------------------------------
# build_geodataframe tests
# ---------------------------------------------------------------------------


def test_build_geodataframe_feature_count() -> None:
    gdf = build_geodataframe.fn(SAMPLE_ORG_UNITS)
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert len(gdf) == 3


def test_build_geodataframe_columns() -> None:
    gdf = build_geodataframe.fn(SAMPLE_ORG_UNITS)
    assert "id" in gdf.columns
    assert "name" in gdf.columns
    assert "geometry" in gdf.columns
    assert "parent_id" in gdf.columns


def test_build_geodataframe_crs() -> None:
    gdf = build_geodataframe.fn(SAMPLE_ORG_UNITS)
    assert gdf.crs is not None
    assert gdf.crs.to_epsg() == 4326


def test_build_geodataframe_geometry_types() -> None:
    gdf = build_geodataframe.fn(SAMPLE_ORG_UNITS)
    types = set(gdf.geometry.geom_type)
    assert "Point" in types
    assert "Polygon" in types


def test_build_geodataframe_parent_id() -> None:
    gdf = build_geodataframe.fn(SAMPLE_ORG_UNITS)
    assert list(gdf["parent_id"]) == ["parent1", "parent1", "parent2"]


def test_build_geodataframe_skips_empty_geometry() -> None:
    data = [
        {"id": "ou1", "name": "A", "geometry": {"type": "Point", "coordinates": [1, 2]}},
        {"id": "ou2", "name": "B"},
        {"id": "ou3", "name": "C", "geometry": None},
    ]
    gdf = build_geodataframe.fn(data)
    assert len(gdf) == 1


# ---------------------------------------------------------------------------
# export_geoparquet tests
# ---------------------------------------------------------------------------


def test_export_geoparquet_produces_bytes() -> None:
    gdf = build_geodataframe.fn(SAMPLE_ORG_UNITS)
    data = export_geoparquet.fn(gdf)
    assert isinstance(data, bytes)
    assert len(data) > 0


def test_export_geoparquet_roundtrip() -> None:
    gdf = build_geodataframe.fn(SAMPLE_ORG_UNITS)
    data = export_geoparquet.fn(gdf)
    gdf_back = gpd.read_parquet(io.BytesIO(data))  # pyright: ignore[reportArgumentType]
    assert len(gdf_back) == len(gdf)
    assert set(gdf_back.columns) == set(gdf.columns)
    assert gdf_back.crs is not None
    assert gdf_back.crs.to_epsg() == 4326


# ---------------------------------------------------------------------------
# upload_to_s3 tests
# ---------------------------------------------------------------------------


@patch("cloud_dhis2_geoparquet_export.S3Bucket")
@patch("cloud_dhis2_geoparquet_export.MinIOCredentials")
def test_upload_to_s3_success(mock_creds_cls: MagicMock, mock_bucket_cls: MagicMock) -> None:
    mock_bucket = MagicMock()
    mock_bucket_cls.return_value = mock_bucket
    mock_creds_cls.return_value = MagicMock()

    key, backend = upload_to_s3.fn(b"fake-geoparquet", "test/file.geoparquet")

    assert key == "test/file.geoparquet"
    assert backend == StorageBackend.S3
    mock_bucket.upload_from_file_object.assert_called_once()


def test_upload_to_s3_fallback() -> None:
    with patch("cloud_dhis2_geoparquet_export.MinIOCredentials", side_effect=Exception("no s3")):
        key, backend = upload_to_s3.fn(b"fake-geoparquet-data", "test/file.geoparquet")

    assert backend == StorageBackend.LOCAL
    assert Path(key).exists()
    assert Path(key).read_bytes() == b"fake-geoparquet-data"


# ---------------------------------------------------------------------------
# build_report tests
# ---------------------------------------------------------------------------


def test_build_report() -> None:
    gdf = build_geodataframe.fn(SAMPLE_ORG_UNITS)
    data = export_geoparquet.fn(gdf)
    report = build_report.fn(gdf, data, "test/key.geoparquet", StorageBackend.S3)

    assert isinstance(report, ExportReport)
    assert report.feature_count == 3
    assert "Point" in report.geometry_type_counts
    assert "Polygon" in report.geometry_type_counts
    assert report.geometry_type_counts["Point"] == 2
    assert report.geometry_type_counts["Polygon"] == 1
    assert len(report.bbox) == 4
    assert report.file_size_bytes == len(data)
    assert report.s3_key == "test/key.geoparquet"
    assert report.storage_backend == StorageBackend.S3


# ---------------------------------------------------------------------------
# fetch_org_units_with_geometry tests
# ---------------------------------------------------------------------------


def test_fetch_org_units_with_geometry() -> None:
    mock_client = MagicMock()
    all_records = [
        {"id": "ou1", "geometry": {"type": "Point", "coordinates": [1, 2]}},
        {"id": "ou2"},
        {"id": "ou3", "geometry": {"type": "Point", "coordinates": [3, 4]}},
    ]
    mock_client.fetch_metadata.return_value = all_records

    result = fetch_org_units_with_geometry.fn(mock_client)
    assert len(result) == 2
    assert all(r.get("geometry") for r in result)


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("cloud_dhis2_geoparquet_export.S3Bucket")
@patch("cloud_dhis2_geoparquet_export.MinIOCredentials")
@patch("cloud_dhis2_geoparquet_export.get_dhis2_credentials")
def test_flow_runs(
    mock_get_creds: MagicMock,
    mock_minio_creds_cls: MagicMock,
    mock_bucket_cls: MagicMock,
) -> None:
    mock_client = MagicMock()
    mock_client.fetch_metadata.return_value = SAMPLE_ORG_UNITS
    mock_creds = MagicMock()
    mock_creds.get_client.return_value = mock_client
    mock_get_creds.return_value = mock_creds

    mock_bucket = MagicMock()
    mock_bucket_cls.return_value = mock_bucket
    mock_minio_creds_cls.return_value = MagicMock()

    state = dhis2_geoparquet_export_flow(instance="test", return_state=True)
    assert state.is_completed()
