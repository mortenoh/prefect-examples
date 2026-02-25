"""DHIS2 Org Units GeoParquet Export to S3.

Fetch org units with geometry from DHIS2, build a GeoDataFrame, and export
to S3-compatible storage (RustFS/MinIO) as GeoParquet. Falls back to a local
file when S3 is unavailable.

Airflow equivalent: PythonOperator + S3Hook.upload_file() with GeoPandas.
Prefect approach:   GeoDataFrame.to_parquet() + S3Bucket block from prefect-aws.
"""

from __future__ import annotations

import io
import logging
import tempfile
from enum import StrEnum
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.runtime import flow_run
from prefect_aws import MinIOCredentials, S3Bucket
from prefect_aws.client_parameters import AwsClientParameters
from pydantic import BaseModel, SecretStr
from shapely.geometry import shape

from prefect_examples.dhis2 import Dhis2Client, get_dhis2_credentials

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class StorageBackend(StrEnum):
    """Where the GeoParquet file ended up."""

    S3 = "s3"
    LOCAL = "local"


class ExportReport(BaseModel):
    """Summary of the GeoParquet export pipeline."""

    feature_count: int
    geometry_type_counts: dict[str, int]
    bbox: list[float]
    file_size_bytes: int
    s3_key: str
    storage_backend: StorageBackend


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def fetch_org_units_with_geometry(client: Dhis2Client) -> list[dict[str, Any]]:
    """Fetch org units with geometry from DHIS2.

    Args:
        client: Authenticated DHIS2 API client.

    Returns:
        List of org unit dicts that have geometry.
    """
    records = client.fetch_metadata(
        "organisationUnits",
        fields="id,name,shortName,level,parent,geometry",
    )
    with_geom = [r for r in records if r.get("geometry")]
    print(f"Fetched {len(records)} org units, {len(with_geom)} with geometry")
    return with_geom


@task
def build_geodataframe(org_units: list[dict[str, Any]]) -> gpd.GeoDataFrame:
    """Convert org unit dicts to a GeoDataFrame.

    Uses ``shapely.geometry.shape()`` to parse DHIS2 GeoJSON geometry into
    Shapely geometry objects.

    Args:
        org_units: Org unit dicts with geometry.

    Returns:
        GeoDataFrame with geometry column set.
    """
    rows: list[dict[str, Any]] = []
    for ou in org_units:
        geom = ou.get("geometry")
        if not geom:
            continue
        parent = ou.get("parent")
        parent_id = parent["id"] if isinstance(parent, dict) else None
        rows.append(
            {
                "id": ou["id"],
                "name": ou.get("name", ""),
                "shortName": ou.get("shortName", ""),
                "level": ou.get("level"),
                "parent_id": parent_id,
                "geometry": shape(geom),
            }
        )

    gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
    print(f"Built GeoDataFrame: {len(gdf)} features, CRS={gdf.crs}")
    return gdf


@task
def export_geoparquet(gdf: gpd.GeoDataFrame) -> bytes:
    """Serialize a GeoDataFrame to GeoParquet bytes.

    Args:
        gdf: GeoDataFrame to export.

    Returns:
        GeoParquet file bytes.
    """
    buf = io.BytesIO()
    gdf.to_parquet(buf, engine="pyarrow", index=False)
    data = buf.getvalue()
    print(f"Serialized GeoParquet: {len(data):,} bytes")
    return data


@task
def upload_to_s3(data: bytes, key: str) -> tuple[str, StorageBackend]:
    """Upload GeoParquet bytes to S3-compatible storage.

    Falls back to writing a local temp file if S3 is not available.

    Args:
        data: GeoParquet file bytes.
        key: S3 object key.

    Returns:
        Tuple of (actual key or local path, backend).
    """
    try:
        minio_creds = MinIOCredentials(
            minio_root_user="admin",
            minio_root_password=SecretStr("admin"),
        )
        bucket = S3Bucket(
            bucket_name="prefect-data",
            credentials=minio_creds,
            bucket_folder="exports",
            aws_client_parameters=AwsClientParameters(endpoint_url="http://localhost:9000"),  # type: ignore[call-arg]
        )
        bucket.upload_from_file_object(io.BytesIO(data), key)
        print(f"Uploaded {len(data):,} bytes to s3://prefect-data/exports/{key}")
        return key, StorageBackend.S3
    except Exception:
        logger.warning("S3 not available, falling back to local temp file")
        tmp_dir = Path(tempfile.gettempdir()) / "prefect-s3-fallback"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = tmp_dir / key.replace("/", "_")
        tmp_path.write_bytes(data)
        print(f"Wrote fallback file: {tmp_path} ({len(data):,} bytes)")
        return str(tmp_path), StorageBackend.LOCAL


@task
def build_report(
    gdf: gpd.GeoDataFrame,
    data: bytes,
    key: str,
    backend: StorageBackend,
) -> ExportReport:
    """Build a summary report for the GeoParquet export.

    Args:
        gdf: The exported GeoDataFrame.
        data: GeoParquet file bytes.
        key: S3 key or local path.
        backend: Where the file was stored.

    Returns:
        ExportReport with geometry type counts and bounding box.
    """
    type_counts: dict[str, int] = {}
    for geom_type in gdf.geometry.geom_type:
        type_counts[geom_type] = type_counts.get(geom_type, 0) + 1

    bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]
    bbox = [round(float(b), 6) for b in bounds]

    return ExportReport(
        feature_count=len(gdf),
        geometry_type_counts=type_counts,
        bbox=bbox,
        file_size_bytes=len(data),
        s3_key=key,
        storage_backend=backend,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="cloud_dhis2_geoparquet_export", log_prints=True)
def dhis2_geoparquet_export_flow(instance: str = "dhis2") -> ExportReport:
    """Fetch DHIS2 org units with geometry and export as GeoParquet to S3.

    Args:
        instance: DHIS2 credentials block name for multi-instance support.

    Returns:
        ExportReport.
    """
    flow_name = flow_run.name or "local"
    timestamp = pd.Timestamp.now().strftime("%Y%m%dT%H%M%S")
    s3_key = f"{flow_name}/{timestamp}.geoparquet"

    creds = get_dhis2_credentials(instance)
    client = creds.get_client()

    org_units = fetch_org_units_with_geometry(client)
    gdf = build_geodataframe(org_units)
    data = export_geoparquet(gdf)
    key, backend = upload_to_s3(data, s3_key)
    report = build_report(gdf, data, key, backend)

    tc = report.geometry_type_counts
    lines = [
        "## DHIS2 GeoParquet Export Summary",
        "",
        f"- **Features:** {report.feature_count}",
        f"- **File size:** {report.file_size_bytes:,} bytes",
        f"- **S3 key:** `{report.s3_key}`",
        f"- **Storage:** {report.storage_backend.value}",
        f"- **Bounding box:** {report.bbox}",
        "",
        "### Geometry Types",
        "",
        "| Type | Count |",
        "|------|-------|",
        *[f"| {k} | {v} |" for k, v in sorted(tc.items())],
    ]
    create_markdown_artifact(key="dhis2-geoparquet-export", markdown="\n".join(lines))

    return report


if __name__ == "__main__":
    dhis2_geoparquet_export_flow()
