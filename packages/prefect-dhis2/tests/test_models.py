"""Tests for DHIS2 response models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from prefect_dhis2 import Dhis2ApiResponse


class TestDhis2ApiResponse:
    """Tests for Dhis2ApiResponse validation."""

    def test_basic_construction(self) -> None:
        resp = Dhis2ApiResponse(endpoint="/api/organisationUnits", record_count=42)
        assert resp.endpoint == "/api/organisationUnits"
        assert resp.record_count == 42
        assert resp.status_code == 200

    def test_custom_status_code(self) -> None:
        resp = Dhis2ApiResponse(endpoint="/api/dataElements", record_count=0, status_code=404)
        assert resp.status_code == 404

    def test_missing_required_fields(self) -> None:
        with pytest.raises(ValidationError):
            Dhis2ApiResponse()  # type: ignore[call-arg]

    def test_missing_endpoint(self) -> None:
        with pytest.raises(ValidationError):
            Dhis2ApiResponse(record_count=5)  # type: ignore[call-arg]

    def test_missing_record_count(self) -> None:
        with pytest.raises(ValidationError):
            Dhis2ApiResponse(endpoint="/api/test")  # type: ignore[call-arg]

    def test_serialization(self) -> None:
        resp = Dhis2ApiResponse(endpoint="/api/indicators", record_count=10, status_code=200)
        data = resp.model_dump()
        assert data == {"endpoint": "/api/indicators", "record_count": 10, "status_code": 200}

    def test_deserialization(self) -> None:
        data = {"endpoint": "/api/programs", "record_count": 3, "status_code": 201}
        resp = Dhis2ApiResponse(**data)
        assert resp.endpoint == "/api/programs"
        assert resp.record_count == 3
        assert resp.status_code == 201
