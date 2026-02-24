"""Tests for flow 109 -- DHIS2 Environment-Based Configuration."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "dhis2_env_config",
    Path(__file__).resolve().parent.parent.parent / "flows" / "dhis2" / "dhis2_env_config.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["dhis2_env_config"] = _mod
_spec.loader.exec_module(_mod)

ConfigSource = _mod.ConfigSource
ConfigReport = _mod.ConfigReport
from_inline_block = _mod.from_inline_block
from_secret_block = _mod.from_secret_block
from_env_var = _mod.from_env_var
from_json_config = _mod.from_json_config
compare_strategies = _mod.compare_strategies
dhis2_env_config_flow = _mod.dhis2_env_config_flow


def test_inline_block() -> None:
    source = from_inline_block.fn()
    assert source.source_type == "inline_block"
    assert "dhis2" in source.value.lower()
    assert source.is_secret is False


def test_secret_block_fallback() -> None:
    source = from_secret_block.fn()
    assert source.source_type == "secret_block"
    assert source.is_secret is True
    assert source.value == "***"


def test_env_var_default() -> None:
    source = from_env_var.fn("DHIS2_TEST_NONEXISTENT", "default_value")
    assert source.source_type == "env_var"
    assert source.value == "default_value"


def test_json_config() -> None:
    source = from_json_config.fn()
    assert source.source_type == "json_block"
    assert "dhis2" in source.value.lower()


def test_json_config_custom() -> None:
    import json

    config = json.dumps({"base_url": "https://custom.dhis2.org"})
    source = from_json_config.fn(config_json=config)
    assert source.value == "https://custom.dhis2.org"


def test_compare_strategies() -> None:
    sources = [
        ConfigSource(source_type="inline_block", key="url", value="x", is_secret=False),
        ConfigSource(source_type="secret_block", key="pass", value="***", is_secret=True),
        ConfigSource(source_type="env_var", key="url", value="y", is_secret=False),
    ]
    report = compare_strategies.fn(sources)
    assert report.strategy_count == 3
    assert len(report.sources) == 3


def test_flow_runs() -> None:
    state = dhis2_env_config_flow(return_state=True)
    assert state.is_completed()
