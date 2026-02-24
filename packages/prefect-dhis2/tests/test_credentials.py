"""Tests for Dhis2Credentials block and Dhis2Client."""

from __future__ import annotations

import pickle
from unittest.mock import MagicMock, patch

from prefect_dhis2 import Dhis2Client, Dhis2Credentials


class TestDhis2Credentials:
    """Tests for the Dhis2Credentials block."""

    def test_default_values(self) -> None:
        creds = Dhis2Credentials()
        assert creds.base_url == "https://play.im.dhis2.org/dev"
        assert creds.username == "admin"
        assert creds.password.get_secret_value() == "district"

    def test_custom_values(self) -> None:
        creds = Dhis2Credentials(
            base_url="https://dhis2.example.org",
            username="user",
            password="secret",
        )
        assert creds.base_url == "https://dhis2.example.org"
        assert creds.username == "user"
        assert creds.password.get_secret_value() == "secret"

    def test_get_client_returns_dhis2_client(self) -> None:
        creds = Dhis2Credentials()
        client = creds.get_client()
        assert isinstance(client, Dhis2Client)
        client.close()

    def test_block_type_slug(self) -> None:
        assert Dhis2Credentials._block_type_slug == "dhis2-credentials"

    def test_block_has_description(self) -> None:
        assert Dhis2Credentials._description is not None
        assert len(Dhis2Credentials._description) > 0

    def test_serialization_round_trip(self) -> None:
        creds = Dhis2Credentials(
            base_url="https://test.dhis2.org",
            username="testuser",
            password="testpass",
        )
        data = creds.model_dump()
        restored = Dhis2Credentials(**data)
        assert restored.base_url == creds.base_url
        assert restored.username == creds.username


class TestDhis2Client:
    """Tests for the Dhis2Client."""

    def test_context_manager(self) -> None:
        with Dhis2Client("https://example.org", "admin", "pass") as client:
            assert isinstance(client, Dhis2Client)

    def test_pickling(self) -> None:
        client = Dhis2Client("https://example.org", "admin", "pass")
        restored = pickle.loads(pickle.dumps(client))
        assert restored._base_url == "https://example.org"
        assert restored._username == "admin"
        assert restored._password == "pass"
        client.close()
        restored.close()

    def test_get_server_info(self) -> None:
        client = Dhis2Client("https://example.org", "admin", "pass")
        mock_response = MagicMock()
        mock_response.json.return_value = {"version": "2.40"}
        with patch.object(client._http, "get", return_value=mock_response) as mock_get:
            result = client.get_server_info()
            mock_get.assert_called_once_with("/system/info")
            assert result == {"version": "2.40"}
        client.close()

    def test_fetch_metadata(self) -> None:
        client = Dhis2Client("https://example.org", "admin", "pass")
        mock_response = MagicMock()
        mock_response.json.return_value = {"organisationUnits": [{"id": "abc"}]}
        with patch.object(client._http, "get", return_value=mock_response):
            result = client.fetch_metadata("organisationUnits")
            assert result == [{"id": "abc"}]
        client.close()

    def test_fetch_analytics(self) -> None:
        client = Dhis2Client("https://example.org", "admin", "pass")
        mock_response = MagicMock()
        mock_response.json.return_value = {"headers": [], "rows": []}
        with patch.object(client._http, "get", return_value=mock_response) as mock_get:
            result = client.fetch_analytics(["dx:uid1"], filter_param="pe:2024")
            assert result == {"headers": [], "rows": []}
            call_kwargs = mock_get.call_args
            assert call_kwargs[1]["params"]["filter"] == "pe:2024"
        client.close()

    def test_fetch_analytics_no_filter(self) -> None:
        client = Dhis2Client("https://example.org", "admin", "pass")
        mock_response = MagicMock()
        mock_response.json.return_value = {"headers": [], "rows": []}
        with patch.object(client._http, "get", return_value=mock_response) as mock_get:
            client.fetch_analytics(["dx:uid1"])
            call_kwargs = mock_get.call_args
            assert "filter" not in call_kwargs[1]["params"]
        client.close()

    def test_mock_spec_pattern(self) -> None:
        """Demonstrate the recommended mocking pattern for downstream tests."""
        mock_client = MagicMock(spec=Dhis2Client)
        mock_client.get_server_info.return_value = {"version": "2.41"}
        assert mock_client.get_server_info() == {"version": "2.41"}
