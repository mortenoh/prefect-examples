"""Tests for flow 073 -- Config-Driven Pipeline."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "data_engineering_config_driven_pipeline",
    Path(__file__).resolve().parent.parent.parent
    / "flows"
    / "data_engineering"
    / "data_engineering_config_driven_pipeline.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["data_engineering_config_driven_pipeline"] = _mod
_spec.loader.exec_module(_mod)

StageConfig = _mod.StageConfig
StageContext = _mod.StageContext
PipelineConfig = _mod.PipelineConfig
StageResult = _mod.StageResult
PipelineResult = _mod.PipelineResult
parse_config = _mod.parse_config
execute_extract = _mod.execute_extract
execute_validate = _mod.execute_validate
execute_transform = _mod.execute_transform
dispatch_stage = _mod.dispatch_stage
config_driven_pipeline_flow = _mod.config_driven_pipeline_flow


def test_stage_config_defaults() -> None:
    s = StageConfig(name="test", task_type="extract")
    assert s.enabled is True


def test_parse_config() -> None:
    raw = {"name": "test", "stages": [{"name": "s1", "task_type": "extract"}]}
    config = parse_config.fn(raw)
    assert isinstance(config, PipelineConfig)
    assert config.name == "test"


def test_execute_extract() -> None:
    result = execute_extract.fn({"count": 5})
    assert isinstance(result, StageContext)
    assert result.count == 5
    assert len(result.records) == 5


def test_execute_validate() -> None:
    context = StageContext(records=[{"id": 1, "value": 10}, {"id": 2, "value": 100}])
    result = execute_validate.fn({"min_value": 50}, context)
    assert isinstance(result, StageContext)
    assert result.count == 1


def test_execute_transform() -> None:
    context = StageContext(records=[{"id": 1, "value": 10}])
    result = execute_transform.fn({"multiplier": 2.0}, context)
    assert isinstance(result, StageContext)
    assert result.records[0]["value"] == 20


def test_dispatch_unknown_type() -> None:
    stage = StageConfig(name="bad", task_type="nonexistent")
    result = dispatch_stage.fn(stage, StageContext())
    assert result.status == "error"


def test_flow_default_config() -> None:
    state = config_driven_pipeline_flow(return_state=True)
    assert state.is_completed()


def test_flow_with_disabled_stage() -> None:
    config = {
        "name": "partial",
        "stages": [
            {"name": "extract", "task_type": "extract", "params": {"count": 5}},
            {"name": "skip_me", "task_type": "transform", "enabled": False},
            {"name": "load", "task_type": "load", "params": {"target": "test_table"}},
        ],
    }
    result = config_driven_pipeline_flow(raw_config=config)
    assert result.stages_executed == 2
    assert result.stages_skipped == 1
