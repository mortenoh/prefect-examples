"""Tests for the dhis2_ou deployment flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "deploy_dhis2_ou",
    Path(__file__).resolve().parent.parent / "deployments" / "dhis2_ou" / "flow.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["deploy_dhis2_ou"] = _mod
_spec.loader.exec_module(_mod)

from prefect_examples.dhis2 import Dhis2Client, Dhis2Credentials
OrgUnitReport = _mod.OrgUnitReport
fetch_org_units = _mod.fetch_org_units
build_report = _mod.build_report
dhis2_ou_flow = _mod.dhis2_ou_flow


@patch.object(Dhis2Client, "fetch_metadata")
def test_fetch_org_units(mock_fetch: MagicMock) -> None:
    mock_fetch.return_value = [
        {"id": "OU1", "displayName": "Org 1"},
        {"id": "OU2", "displayName": "Org 2"},
    ]
    client = MagicMock(spec=Dhis2Client)
    client.fetch_metadata = mock_fetch
    result = fetch_org_units.fn(client)
    assert len(result) == 2
    assert result[0]["id"] == "OU1"
    assert result[0]["displayName"] == "Org 1"


def test_build_report() -> None:
    org_units = [
        {"id": "OU1", "displayName": "Org 1"},
        {"id": "OU2", "displayName": "Org 2"},
        {"id": "OU3", "displayName": "Org 3"},
    ]
    report = build_report.fn(org_units)
    assert isinstance(report, OrgUnitReport)
    assert report.org_unit_count == 3
    assert "OU1" in report.markdown
    assert "Org 1" in report.markdown
    assert "OU3" in report.markdown
    assert "**Total:** 3 units" in report.markdown


@patch.object(Dhis2Credentials, "get_client")
def test_flow_runs(mock_get_client: MagicMock) -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = [
        {"id": "OU1", "displayName": "Org 1"},
        {"id": "OU2", "displayName": "Org 2"},
    ]
    mock_get_client.return_value = mock_client
    state = dhis2_ou_flow(return_state=True)
    assert state.is_completed()
    report = state.result()
    assert isinstance(report, OrgUnitReport)
    assert report.org_unit_count == 2
