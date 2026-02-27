"""Tests for WorldPop Dataset Catalog flow."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location(
    "cloud_worldpop_dataset_catalog",
    Path(__file__).resolve().parent.parent.parent / "flows" / "cloud" / "cloud_worldpop_dataset_catalog.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["cloud_worldpop_dataset_catalog"] = _mod
_spec.loader.exec_module(_mod)

DatasetQuery = _mod.DatasetQuery
DatasetRecord = _mod.DatasetRecord
SubDatasetRecord = _mod.SubDatasetRecord
CountryDatasetRecord = _mod.CountryDatasetRecord
CatalogReport = _mod.CatalogReport
list_datasets = _mod.list_datasets
list_subdatasets = _mod.list_subdatasets
query_country_datasets = _mod.query_country_datasets
build_catalog_report = _mod.build_catalog_report
worldpop_dataset_catalog_flow = _mod.worldpop_dataset_catalog_flow


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_dataset_query_defaults() -> None:
    q = DatasetQuery()
    assert q.dataset is None
    assert q.subdataset is None
    assert q.iso3 is None


def test_dataset_query_with_values() -> None:
    q = DatasetQuery(dataset="pop", subdataset="wpgp", iso3="ETH")
    assert q.dataset == "pop"
    assert q.subdataset == "wpgp"
    assert q.iso3 == "ETH"


def test_dataset_record_model() -> None:
    r = DatasetRecord(id="pop", title="Population", description="Gridded population")
    assert r.id == "pop"
    assert r.title == "Population"


def test_country_dataset_record_model() -> None:
    r = CountryDatasetRecord(
        title="Ethiopia 2020",
        iso3="ETH",
        popyear="2020",
        doi="10.5258/SOTON/WP00670",
    )
    assert r.iso3 == "ETH"
    assert r.popyear == "2020"


# ---------------------------------------------------------------------------
# Task tests
# ---------------------------------------------------------------------------


@patch("cloud_worldpop_dataset_catalog.httpx.Client")
def test_list_datasets(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "data": [
            {"alias": "pop", "title": "Population", "desc": "Gridded population"},
            {"alias": "cov", "title": "Covariates", "desc": "Geospatial covariates"},
        ]
    }
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = list_datasets.fn()
    assert len(result) == 2
    assert result[0].id == "pop"
    assert result[1].id == "cov"


@patch("cloud_worldpop_dataset_catalog.httpx.Client")
def test_list_subdatasets(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "data": [
            {"alias": "wpgp", "title": "Unconstrained", "desc": "UN adjusted"},
            {"alias": "wpgpas", "title": "Age structures", "desc": "Age-sex"},
        ]
    }
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = list_subdatasets.fn("pop")
    assert len(result) == 2
    assert result[0].id == "wpgp"


@patch("cloud_worldpop_dataset_catalog.httpx.Client")
def test_query_country_datasets(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "data": [
            {
                "title": "Ethiopia 2020",
                "iso3": "ETH",
                "popyear": 2020,
                "doi": "10.5258/SOTON/WP00670",
                "url_summary": "https://hub.worldpop.org/...",
            }
        ]
    }
    mock_client = MagicMock()
    mock_client.get.return_value = mock_resp
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    result = query_country_datasets.fn("pop", "wpgp", "ETH")
    assert len(result) == 1
    assert result[0].iso3 == "ETH"
    assert result[0].popyear == "2020"


def test_build_catalog_report() -> None:
    datasets = [DatasetRecord(id="pop", title="Population")]
    subdatasets = [SubDatasetRecord(id="wpgp", title="Unconstrained")]
    country_records = [CountryDatasetRecord(title="Ethiopia 2020", iso3="ETH", popyear="2020")]
    query = DatasetQuery(dataset="pop", subdataset="wpgp", iso3="ETH")

    report = build_catalog_report.fn(datasets, subdatasets, country_records, query)

    assert isinstance(report, CatalogReport)
    assert report.datasets_found == 1
    assert report.subdatasets_found == 1
    assert report.country_records_found == 1
    assert "Ethiopia 2020" in report.markdown
    assert "| pop |" in report.markdown


def test_build_catalog_report_no_country() -> None:
    datasets = [DatasetRecord(id="pop", title="Population")]
    query = DatasetQuery()

    report = build_catalog_report.fn(datasets, [], [], query)

    assert report.datasets_found == 1
    assert report.subdatasets_found == 0
    assert report.country_records_found == 0


# ---------------------------------------------------------------------------
# Flow integration test
# ---------------------------------------------------------------------------


@patch("cloud_worldpop_dataset_catalog.httpx.Client")
def test_flow_runs(mock_client_cls: MagicMock) -> None:
    call_count = 0

    def fake_get(url: str, **kwargs: object) -> MagicMock:
        nonlocal call_count
        call_count += 1
        resp = MagicMock()
        if call_count == 1:
            resp.json.return_value = {"data": [{"alias": "pop", "title": "Population", "desc": ""}]}
        elif call_count == 2:
            resp.json.return_value = {"data": [{"alias": "wpgp", "title": "Unconstrained", "desc": ""}]}
        else:
            resp.json.return_value = {
                "data": [{"title": "ETH 2020", "iso3": "ETH", "popyear": 2020, "doi": "", "url_summary": ""}]
            }
        return resp

    mock_client = MagicMock()
    mock_client.get.side_effect = fake_get
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client

    state = worldpop_dataset_catalog_flow(return_state=True)
    assert state.is_completed()
