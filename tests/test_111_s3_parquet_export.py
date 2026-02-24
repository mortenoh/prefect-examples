"""Tests for flow 111 -- S3 Parquet Export."""

import importlib.util
import io
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

_spec = importlib.util.spec_from_file_location(
    "flow_111",
    Path(__file__).resolve().parent.parent / "flows" / "111_s3_parquet_export.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_111"] = _mod
_spec.loader.exec_module(_mod)

SensorReading = _mod.SensorReading
StationId = _mod.StationId
OperationalStatus = _mod.OperationalStatus
TempCategory = _mod.TempCategory
TransformResult = _mod.TransformResult
UploadResult = _mod.UploadResult
StorageBackend = _mod.StorageBackend
ExportResult = _mod.ExportResult
generate_records = _mod.generate_records
transform_to_dataframe = _mod.transform_to_dataframe
upload_to_s3 = _mod.upload_to_s3
verify_upload = _mod.verify_upload
s3_parquet_export_flow = _mod.s3_parquet_export_flow


# ---------------------------------------------------------------------------
# SensorReading model tests
# ---------------------------------------------------------------------------


def test_sensor_reading_heat_index() -> None:
    reading = SensorReading(
        station=StationId.ALPHA,
        date="2024-06-15",
        temperature_c=30.0,
        humidity_pct=80.0,
        status=OperationalStatus.OPERATIONAL,
    )
    assert reading.heat_index == 34.0


def test_sensor_reading_temp_category_cold() -> None:
    reading = SensorReading(
        station=StationId.BETA,
        date="2024-01-15",
        temperature_c=-5.0,
        humidity_pct=50.0,
        status=OperationalStatus.OPERATIONAL,
    )
    assert reading.temp_category == TempCategory.COLD


def test_sensor_reading_temp_category_mild() -> None:
    reading = SensorReading(
        station=StationId.BETA,
        date="2024-03-15",
        temperature_c=10.0,
        humidity_pct=50.0,
        status=OperationalStatus.OPERATIONAL,
    )
    assert reading.temp_category == TempCategory.MILD


def test_sensor_reading_temp_category_warm() -> None:
    reading = SensorReading(
        station=StationId.GAMMA,
        date="2024-06-15",
        temperature_c=25.0,
        humidity_pct=50.0,
        status=OperationalStatus.MAINTENANCE,
    )
    assert reading.temp_category == TempCategory.WARM


def test_sensor_reading_temp_category_hot() -> None:
    reading = SensorReading(
        station=StationId.DELTA,
        date="2024-08-01",
        temperature_c=35.0,
        humidity_pct=50.0,
        status=OperationalStatus.DEGRADED,
    )
    assert reading.temp_category == TempCategory.HOT


def test_sensor_reading_model_dump() -> None:
    reading = SensorReading(
        station=StationId.ALPHA,
        date="2024-01-01",
        temperature_c=20.0,
        humidity_pct=60.0,
        status=OperationalStatus.OPERATIONAL,
    )
    data = reading.model_dump()
    assert "heat_index" in data
    assert "temp_category" in data
    assert data["station"] == "station-alpha"
    assert data["status"] == "operational"


# ---------------------------------------------------------------------------
# generate_records tests
# ---------------------------------------------------------------------------


def test_generate_records_count() -> None:
    records = generate_records.fn(50)
    assert len(records) == 50


def test_generate_records_returns_pydantic_models() -> None:
    records = generate_records.fn(5)
    for rec in records:
        assert isinstance(rec, SensorReading)


def test_generate_records_value_ranges() -> None:
    records = generate_records.fn(100)
    for rec in records:
        assert -10.0 <= rec.temperature_c <= 45.0
        assert 10.0 <= rec.humidity_pct <= 100.0
        assert isinstance(rec.station, StationId)
        assert isinstance(rec.status, OperationalStatus)


# ---------------------------------------------------------------------------
# transform_to_dataframe tests
# ---------------------------------------------------------------------------


def test_transform_produces_parquet() -> None:
    records = generate_records.fn(10)
    result = transform_to_dataframe.fn(records)
    assert isinstance(result, TransformResult)
    assert result.row_count == 10
    assert result.parquet_size_bytes > 0
    df = pd.read_parquet(io.BytesIO(result.parquet_data))
    assert len(df) == 10


def test_transform_includes_computed_columns() -> None:
    records = generate_records.fn(20)
    result = transform_to_dataframe.fn(records)
    assert "heat_index" in result.columns
    assert "temp_category" in result.columns


def test_transform_preserves_temp_categories() -> None:
    readings = [
        SensorReading(
            station=StationId.ALPHA, date="2024-01-01", temperature_c=-5.0,
            humidity_pct=50.0, status=OperationalStatus.OPERATIONAL,
        ),
        SensorReading(
            station=StationId.BETA, date="2024-01-02", temperature_c=10.0,
            humidity_pct=50.0, status=OperationalStatus.OPERATIONAL,
        ),
        SensorReading(
            station=StationId.GAMMA, date="2024-01-03", temperature_c=25.0,
            humidity_pct=50.0, status=OperationalStatus.OPERATIONAL,
        ),
        SensorReading(
            station=StationId.DELTA, date="2024-01-04", temperature_c=35.0,
            humidity_pct=50.0, status=OperationalStatus.OPERATIONAL,
        ),
    ]
    result = transform_to_dataframe.fn(readings)
    df = pd.read_parquet(io.BytesIO(result.parquet_data))
    categories = list(df["temp_category"])
    assert categories == ["cold", "mild", "warm", "hot"]


# ---------------------------------------------------------------------------
# upload_to_s3 tests
# ---------------------------------------------------------------------------


@patch("flow_111.S3Bucket")
@patch("flow_111.MinIOCredentials")
def test_upload_to_s3_success(mock_creds_cls: MagicMock, mock_bucket_cls: MagicMock) -> None:
    mock_bucket = MagicMock()
    mock_bucket_cls.return_value = mock_bucket
    mock_creds_cls.return_value = MagicMock()

    transform = TransformResult(
        row_count=5, column_count=7, parquet_size_bytes=100,
        columns=["a"], parquet_data=b"fake-parquet",
    )
    result = upload_to_s3.fn(transform, "test/file.parquet")

    assert isinstance(result, UploadResult)
    assert result.key == "test/file.parquet"
    assert result.backend == StorageBackend.S3
    mock_bucket.upload_from_file_object.assert_called_once()


def test_upload_to_s3_fallback() -> None:
    transform = TransformResult(
        row_count=5, column_count=7, parquet_size_bytes=17,
        columns=["a"], parquet_data=b"fake-parquet-data",
    )
    with patch("flow_111.MinIOCredentials", side_effect=Exception("no s3")):
        result = upload_to_s3.fn(transform, "test/file.parquet")

    assert isinstance(result, UploadResult)
    assert result.backend == StorageBackend.LOCAL
    assert Path(result.key).exists()
    assert Path(result.key).read_bytes() == b"fake-parquet-data"


# ---------------------------------------------------------------------------
# verify_upload tests
# ---------------------------------------------------------------------------


def test_verify_upload_local_file(tmp_path: Path) -> None:
    records = generate_records.fn(10)
    transform = transform_to_dataframe.fn(records)

    local_file = tmp_path / "test.parquet"
    local_file.write_bytes(transform.parquet_data)

    upload = UploadResult(key=str(local_file), size_bytes=transform.parquet_size_bytes, backend=StorageBackend.LOCAL)
    result = verify_upload.fn(upload, transform)

    assert isinstance(result, ExportResult)
    assert result.records_exported == 10
    assert result.storage_backend == StorageBackend.LOCAL
    assert result.file_size_bytes == transform.parquet_size_bytes
    assert len(result.category_distribution) > 0
    assert len(result.station_distribution) > 0
    assert result.temp_min <= result.temp_mean <= result.temp_max


def test_verify_upload_unverified() -> None:
    records = generate_records.fn(5)
    transform = transform_to_dataframe.fn(records)

    upload = UploadResult(key="nonexistent/key.parquet", size_bytes=100, backend=StorageBackend.LOCAL)
    result = verify_upload.fn(upload, transform)

    assert result.storage_backend == StorageBackend.UNVERIFIED
    assert result.records_exported == 5


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("flow_111.S3Bucket")
@patch("flow_111.MinIOCredentials")
def test_flow_runs(mock_creds_cls: MagicMock, mock_bucket_cls: MagicMock) -> None:
    # Capture uploaded bytes so the download mock can replay them
    uploaded_data: list[bytes] = []

    def fake_upload(file_obj: io.BytesIO, key: str) -> None:
        uploaded_data.append(file_obj.read())

    def fake_download(key: str, file_obj: io.BytesIO) -> None:
        if uploaded_data:
            file_obj.write(uploaded_data[0])
            file_obj.seek(0)

    mock_bucket = MagicMock()
    mock_bucket.upload_from_file_object.side_effect = fake_upload
    mock_bucket.download_object_to_file_object.side_effect = fake_download
    mock_bucket_cls.return_value = mock_bucket
    mock_creds_cls.return_value = MagicMock()

    state = s3_parquet_export_flow(n_records=10, return_state=True)
    assert state.is_completed()
