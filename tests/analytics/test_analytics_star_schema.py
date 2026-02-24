"""Tests for flow 090 -- Star Schema."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "analytics_star_schema",
    Path(__file__).resolve().parent.parent.parent / "flows" / "analytics" / "analytics_star_schema.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["analytics_star_schema"] = _mod
_spec.loader.exec_module(_mod)

DimCountry = _mod.DimCountry
DimTime = _mod.DimTime
DimIndicator = _mod.DimIndicator
FactRecord = _mod.FactRecord
CompositeRanking = _mod.CompositeRanking
StarSchemaReport = _mod.StarSchemaReport
build_country_dimension = _mod.build_country_dimension
build_time_dimension = _mod.build_time_dimension
build_indicator_dimension = _mod.build_indicator_dimension
build_fact_table = _mod.build_fact_table
min_max_normalize = _mod.min_max_normalize
compute_composite_index = _mod.compute_composite_index
star_schema_flow = _mod.star_schema_flow


def test_build_country_dimension() -> None:
    data = [{"name": "A", "region": "R1", "population": 1000}]
    dims = build_country_dimension.fn(data)
    assert len(dims) == 1
    assert dims[0].key == 1


def test_surrogate_key_uniqueness() -> None:
    data = [
        {"name": "A", "region": "R1", "population": 1000},
        {"name": "B", "region": "R2", "population": 2000},
        {"name": "C", "region": "R1", "population": 3000},
    ]
    dims = build_country_dimension.fn(data)
    keys = [d.key for d in dims]
    assert len(keys) == len(set(keys))


def test_build_time_dimension() -> None:
    dims = build_time_dimension.fn(2020, 2022)
    assert len(dims) == 3
    assert dims[0].year == 2020
    assert dims[0].decade == 2020
    assert dims[0].is_21st_century is True


def test_build_fact_table() -> None:
    countries = build_country_dimension.fn([{"name": "A", "region": "R", "population": 1000}])
    times = build_time_dimension.fn(2020, 2022)
    indicators = build_indicator_dimension.fn([{"name": "X", "unit": "u", "higher_is_better": True}])
    facts = build_fact_table.fn(countries, times, indicators)
    assert len(facts) == 3  # 1 country * 3 years * 1 indicator


def test_min_max_normalize() -> None:
    values = [10.0, 20.0, 30.0]
    result = min_max_normalize.fn(values, higher_is_better=True)
    assert result[0] == 0.0
    assert result[-1] == 1.0


def test_min_max_normalize_inverted() -> None:
    values = [10.0, 20.0, 30.0]
    result = min_max_normalize.fn(values, higher_is_better=False)
    assert result[0] == 1.0  # Lowest raw = best = 1.0
    assert result[-1] == 0.0


def test_composite_index_ordering() -> None:
    countries = build_country_dimension.fn(
        [
            {"name": "A", "region": "R", "population": 1000},
            {"name": "B", "region": "R", "population": 2000},
        ]
    )
    times = build_time_dimension.fn(2022, 2022)
    indicators = build_indicator_dimension.fn(
        [
            {"name": "X", "unit": "u", "higher_is_better": True},
        ]
    )
    facts = build_fact_table.fn(countries, times, indicators)
    composite = compute_composite_index.fn(facts, indicators, countries, times, {"X": 1.0})
    assert len(composite) == 2
    assert all(isinstance(c, CompositeRanking) for c in composite)
    assert composite[0].composite_score >= composite[1].composite_score


def test_flow_runs() -> None:
    state = star_schema_flow(return_state=True)
    assert state.is_completed()
