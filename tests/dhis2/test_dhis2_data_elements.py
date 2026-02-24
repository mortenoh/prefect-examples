"""Tests for flow 103 -- DHIS2 Data Elements API."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "dhis2_data_elements",
    Path(__file__).resolve().parent.parent.parent / "flows" / "dhis2" / "dhis2_data_elements.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["dhis2_data_elements"] = _mod
_spec.loader.exec_module(_mod)

from prefect_examples.dhis2 import Dhis2Client, Dhis2Credentials  # noqa: E402

FlatDataElement = _mod.FlatDataElement
DataElementReport = _mod.DataElementReport
fetch_data_elements = _mod.fetch_data_elements
flatten_data_elements = _mod.flatten_data_elements
write_data_element_csv = _mod.write_data_element_csv
data_element_report = _mod.data_element_report
dhis2_data_elements_flow = _mod.dhis2_data_elements_flow

SAMPLE_DATA_ELEMENTS = [
    {
        "id": "DE001",
        "name": "ANC 1st visit",
        "shortName": "ANC1",
        "domainType": "AGGREGATE",
        "valueType": "NUMBER",
        "aggregationType": "SUM",
        "categoryCombo": {"id": "CC001", "name": "default"},
        "code": "ANC_1",
    },
    {
        "id": "DE002",
        "name": "Malaria cases",
        "shortName": "Mal",
        "domainType": "AGGREGATE",
        "valueType": "INTEGER",
        "aggregationType": "SUM",
        "categoryCombo": {"id": "CC002", "name": "age"},
        "code": None,
    },
    {
        "id": "DE003",
        "name": "Birth date",
        "shortName": "BD",
        "domainType": "TRACKER",
        "valueType": "DATE",
        "aggregationType": "NONE",
        "categoryCombo": None,
        "code": "BIRTH_DT",
    },
]


@patch.object(Dhis2Client, "fetch_metadata")
def test_fetch_data_elements(mock_fetch: MagicMock) -> None:
    mock_fetch.return_value = SAMPLE_DATA_ELEMENTS
    client = MagicMock(spec=Dhis2Client)
    client.fetch_metadata = mock_fetch
    elements = fetch_data_elements.fn(client)
    assert len(elements) == 3


def test_flatten_data_elements() -> None:
    flat = flatten_data_elements.fn(SAMPLE_DATA_ELEMENTS)
    assert len(flat) == 3
    assert all(isinstance(e, FlatDataElement) for e in flat)


def test_category_combo_extraction() -> None:
    flat = flatten_data_elements.fn(SAMPLE_DATA_ELEMENTS)
    de1 = next(e for e in flat if e.id == "DE001")
    de3 = next(e for e in flat if e.id == "DE003")
    assert de1.category_combo_id == "CC001"
    assert de3.category_combo_id == ""


def test_has_code_logic() -> None:
    flat = flatten_data_elements.fn(SAMPLE_DATA_ELEMENTS)
    coded = [e for e in flat if e.has_code]
    uncoded = [e for e in flat if not e.has_code]
    assert len(coded) == 2  # DE001 and DE003
    assert len(uncoded) == 1  # DE002


def test_name_length() -> None:
    flat = flatten_data_elements.fn(SAMPLE_DATA_ELEMENTS)
    for e in flat:
        assert e.name_length == len(e.name)


def test_report_code_coverage() -> None:
    flat = flatten_data_elements.fn(SAMPLE_DATA_ELEMENTS)
    report = data_element_report.fn(flat)
    assert 0.0 <= report.code_coverage <= 1.0
    assert report.total == 3


def test_write_csv(tmp_path: Path) -> None:
    flat = flatten_data_elements.fn(SAMPLE_DATA_ELEMENTS)
    path = write_data_element_csv.fn(flat, str(tmp_path))
    assert path.exists()
    assert path.name == "data_elements.csv"


@patch.object(Dhis2Credentials, "get_client")
def test_flow_runs(mock_get_client: MagicMock, tmp_path: Path) -> None:
    mock_client = MagicMock(spec=Dhis2Client)
    mock_client.fetch_metadata.return_value = SAMPLE_DATA_ELEMENTS
    mock_get_client.return_value = mock_client
    state = dhis2_data_elements_flow(output_dir=str(tmp_path), return_state=True)
    assert state.is_completed()
