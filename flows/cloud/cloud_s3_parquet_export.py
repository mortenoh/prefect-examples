"""S3 Parquet Export.

Generate sample data, transform with pandas, and write a parquet file
to S3-compatible storage (RustFS/MinIO).

Airflow equivalent: PythonOperator + S3Hook.upload_file().
Prefect approach:   pandas DataFrame + S3Bucket block from prefect-aws.
"""

from __future__ import annotations

import io
import logging
import random
import tempfile
from datetime import date, timedelta
from enum import StrEnum
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.runtime import flow_run
from prefect_aws import MinIOCredentials, S3Bucket
from prefect_aws.client_parameters import AwsClientParameters
from pydantic import BaseModel, Field, SecretStr, computed_field

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class StationId(StrEnum):
    """Known sensor station identifiers."""

    ALPHA = "station-alpha"
    BETA = "station-beta"
    GAMMA = "station-gamma"
    DELTA = "station-delta"


class OperationalStatus(StrEnum):
    """Station operational status."""

    OPERATIONAL = "operational"
    MAINTENANCE = "maintenance"
    DEGRADED = "degraded"


class TempCategory(StrEnum):
    """Temperature classification bins."""

    COLD = "cold"
    MILD = "mild"
    WARM = "warm"
    HOT = "hot"


class SensorReading(BaseModel):
    """A single daily sensor reading from a weather station."""

    station: StationId
    date: date
    temperature_c: float = Field(ge=-50.0, le=60.0)
    humidity_pct: float = Field(ge=0.0, le=100.0)
    status: OperationalStatus

    @computed_field  # type: ignore[prop-decorator]
    @property
    def heat_index(self) -> float:
        """Simplified heat index approximation."""
        return round(self.temperature_c + 0.05 * self.humidity_pct, 1)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def temp_category(self) -> TempCategory:
        """Classify temperature into bins."""
        if self.temperature_c <= 0.0:
            return TempCategory.COLD
        if self.temperature_c <= 15.0:
            return TempCategory.MILD
        if self.temperature_c <= 30.0:
            return TempCategory.WARM
        return TempCategory.HOT


class TransformResult(BaseModel):
    """Outcome of the DataFrame transformation step."""

    row_count: int
    column_count: int
    parquet_size_bytes: int
    columns: list[str]
    parquet_data: bytes

    model_config = {"arbitrary_types_allowed": True}


class StorageBackend(StrEnum):
    """Where the parquet file ended up."""

    S3 = "s3"
    LOCAL = "local"
    UNVERIFIED = "unverified"


class UploadResult(BaseModel):
    """Outcome of the S3 upload step."""

    key: str
    size_bytes: int
    backend: StorageBackend


class ExportResult(BaseModel):
    """Full pipeline summary."""

    records_generated: int
    records_exported: int
    s3_key: str
    file_size_bytes: int
    columns: list[str]
    storage_backend: StorageBackend
    category_distribution: dict[str, int]
    station_distribution: dict[str, int]
    temp_min: float
    temp_max: float
    temp_mean: float


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def generate_records(n: int) -> list[SensorReading]:
    """Generate sample daily sensor readings as validated Pydantic models.

    Args:
        n: Number of records to generate.

    Returns:
        List of SensorReading instances.
    """
    rng = random.Random(42)
    base_date = date(2024, 1, 1)
    stations = list(StationId)
    statuses = list(OperationalStatus)

    readings: list[SensorReading] = []
    for i in range(n):
        readings.append(
            SensorReading(
                station=rng.choice(stations),
                date=base_date + timedelta(days=i % 365),
                temperature_c=round(rng.uniform(-10.0, 45.0), 1),
                humidity_pct=round(rng.uniform(10.0, 100.0), 1),
                status=rng.choice(statuses),
            )
        )
    print(f"Generated {len(readings)} sensor readings (Pydantic-validated)")
    return readings


@task
def transform_to_dataframe(readings: list[SensorReading]) -> TransformResult:
    """Convert validated readings to a pandas DataFrame and serialize to parquet.

    The DataFrame includes all model fields plus computed properties
    (heat_index, temp_category).

    Args:
        readings: Validated SensorReading models.

    Returns:
        TransformResult with parquet bytes and metadata.
    """
    rows = [r.model_dump() for r in readings]
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])

    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    data = buf.getvalue()

    result = TransformResult(
        row_count=len(df),
        column_count=len(df.columns),
        parquet_size_bytes=len(data),
        columns=list(df.columns),
        parquet_data=data,
    )
    print(f"Transformed: {result.row_count} rows, {result.column_count} cols -> {result.parquet_size_bytes} bytes")
    return result


@task
def upload_to_s3(transform: TransformResult, key: str) -> UploadResult:
    """Upload parquet bytes to S3-compatible storage (RustFS/MinIO).

    Creates MinIOCredentials and S3Bucket blocks inline. Falls back to
    writing a local temp file if S3 is not available.

    Args:
        transform: TransformResult containing parquet bytes.
        key: S3 object key.

    Returns:
        UploadResult with key and backend info.
    """
    data = transform.parquet_data
    try:
        minio_creds = MinIOCredentials(
            minio_root_user="admin",
            minio_root_password=SecretStr("admin"),
        )
        bucket = S3Bucket(
            bucket_name="prefect-data",
            credentials=minio_creds,
            bucket_folder="exports",
            aws_client_parameters=AwsClientParameters(endpoint_url="http://localhost:9000"),
        )
        bucket.upload_from_file_object(io.BytesIO(data), key)
        print(f"Uploaded {len(data)} bytes to s3://prefect-data/exports/{key}")
        return UploadResult(key=key, size_bytes=len(data), backend=StorageBackend.S3)
    except Exception:
        logger.warning("S3 not available, falling back to local temp file")
        tmp_dir = Path(tempfile.gettempdir()) / "prefect-s3-fallback"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = tmp_dir / key.replace("/", "_")
        tmp_path.write_bytes(data)
        print(f"Wrote fallback file: {tmp_path} ({len(data)} bytes)")
        return UploadResult(key=str(tmp_path), size_bytes=len(data), backend=StorageBackend.LOCAL)


@task
def verify_upload(upload: UploadResult, transform: TransformResult) -> ExportResult:
    """Verify the upload by reading back and computing summary statistics.

    Args:
        upload: UploadResult from the upload step.
        transform: TransformResult for fallback data access.

    Returns:
        ExportResult with full pipeline metadata.
    """
    local_path = Path(upload.key)
    if local_path.exists():
        data = local_path.read_bytes()
        backend = StorageBackend.LOCAL
    elif upload.backend == StorageBackend.S3:
        try:
            minio_creds = MinIOCredentials(
                minio_root_user="admin",
                minio_root_password=SecretStr("admin"),
            )
            bucket = S3Bucket(
                bucket_name="prefect-data",
                credentials=minio_creds,
                bucket_folder="exports",
                aws_client_parameters=AwsClientParameters(endpoint_url="http://localhost:9000"),
            )
            buf = io.BytesIO()
            bucket.download_object_to_file_object(upload.key, buf)
            data = buf.getvalue()
            backend = StorageBackend.S3
        except Exception:
            data = transform.parquet_data
            backend = StorageBackend.UNVERIFIED
    else:
        data = transform.parquet_data
        backend = StorageBackend.UNVERIFIED

    df = pd.read_parquet(io.BytesIO(data))

    category_dist: dict[str, int] = {}
    if "temp_category" in df.columns:
        category_dist = dict(df["temp_category"].value_counts())

    station_dist: dict[str, int] = {}
    if "station" in df.columns:
        station_dist = dict(df["station"].value_counts())

    result = ExportResult(
        records_generated=transform.row_count,
        records_exported=len(df),
        s3_key=upload.key,
        file_size_bytes=len(data),
        columns=list(df.columns),
        storage_backend=backend,
        category_distribution=category_dist,
        station_distribution=station_dist,
        temp_min=float(df["temperature_c"].min()) if "temperature_c" in df.columns else 0.0,
        temp_max=float(df["temperature_c"].max()) if "temperature_c" in df.columns else 0.0,
        temp_mean=round(float(df["temperature_c"].mean()), 1) if "temperature_c" in df.columns else 0.0,
    )
    print(f"Verified: {result.records_exported} rows, {result.file_size_bytes} bytes ({result.storage_backend.value})")
    return result


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="cloud_s3_parquet_export", log_prints=True)
def s3_parquet_export_flow(n_records: int = 500) -> ExportResult:
    """Generate sample data, transform with pandas, and export to S3 as parquet.

    Args:
        n_records: Number of sensor records to generate.

    Returns:
        ExportResult.
    """
    flow_name = flow_run.name or "local"
    timestamp = pd.Timestamp.now().strftime("%Y%m%dT%H%M%S")
    s3_key = f"{flow_name}/{timestamp}.parquet"

    readings = generate_records(n_records)
    transform = transform_to_dataframe(readings)
    upload = upload_to_s3(transform, s3_key)
    result = verify_upload(upload, transform)

    cat = result.category_distribution
    sta = result.station_distribution
    lines = [
        "## S3 Parquet Export Summary",
        "",
        f"- **Records:** {result.records_generated} generated, {result.records_exported} exported",
        f"- **File size:** {result.file_size_bytes:,} bytes",
        f"- **S3 key:** `{result.s3_key}`",
        f"- **Storage:** {result.storage_backend.value}",
        f"- **Temperature:** min={result.temp_min}, max={result.temp_max}, mean={result.temp_mean}",
        "",
        "### Temperature Categories",
        "",
        "| Category | Count |",
        "|----------|-------|",
        *[f"| {k} | {v} |" for k, v in sorted(cat.items())],
        "",
        "### Station Distribution",
        "",
        "| Station | Count |",
        "|---------|-------|",
        *[f"| {k} | {v} |" for k, v in sorted(sta.items())],
    ]
    create_markdown_artifact(key="s3-parquet-export", markdown="\n".join(lines))

    return result


if __name__ == "__main__":
    load_dotenv()
    s3_parquet_export_flow()
