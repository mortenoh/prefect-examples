"""Tests for flow 098 -- Pipeline Template Factory."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "flow_098",
    Path(__file__).resolve().parent.parent / "flows" / "098_pipeline_template.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["flow_098"] = _mod
_spec.loader.exec_module(_mod)

StageTemplate = _mod.StageTemplate
PipelineTemplate = _mod.PipelineTemplate
StageResult = _mod.StageResult
TemplateInstance = _mod.TemplateInstance
FactoryReport = _mod.FactoryReport
create_template = _mod.create_template
instantiate_template = _mod.instantiate_template
execute_stage = _mod.execute_stage
run_instance = _mod.run_instance
factory_report = _mod.factory_report
pipeline_template_flow = _mod.pipeline_template_flow


def test_create_template() -> None:
    stages = [StageTemplate(name="extract", stage_type="extract", default_params={"batch_size": 100})]
    template = create_template.fn("test", stages)
    assert isinstance(template, PipelineTemplate)
    assert template.template_name == "test"
    assert len(template.stages) == 1


def test_instantiate_template() -> None:
    stages = [StageTemplate(name="extract", stage_type="extract", default_params={"batch_size": 100})]
    template = create_template.fn("test", stages)
    instance = instantiate_template.fn(template, {"batch_size": 50})
    assert instance.template_name == "test"
    assert instance.overrides["batch_size"] == 50
    assert len(instance.results) == 0


def test_execute_stage_extract() -> None:
    stage = StageTemplate(name="extract", stage_type="extract", default_params={"batch_size": 100})
    result = execute_stage.fn(stage, {"batch_size": 200})
    assert result.records_out == 200
    assert result.params_used["batch_size"] == 200


def test_override_application() -> None:
    stage = StageTemplate(name="extract", stage_type="extract", default_params={"batch_size": 100})
    default_result = execute_stage.fn(stage, {})
    override_result = execute_stage.fn(stage, {"batch_size": 500})
    assert default_result.records_out == 100
    assert override_result.records_out == 500


def test_run_instance() -> None:
    stages = [
        StageTemplate(name="extract", stage_type="extract", default_params={"batch_size": 100}),
        StageTemplate(name="load", stage_type="load", default_params={"batch_size": 100}),
    ]
    template = create_template.fn("test", stages)
    instance = instantiate_template.fn(template, {})
    executed = run_instance.fn(instance, template)
    assert len(executed.results) == 2


def test_template_reuse() -> None:
    stages = [StageTemplate(name="extract", stage_type="extract", default_params={"batch_size": 100})]
    template = create_template.fn("reusable", stages)
    inst1 = instantiate_template.fn(template, {"batch_size": 50})
    inst2 = instantiate_template.fn(template, {"batch_size": 200})
    r1 = run_instance.fn(inst1, template)
    r2 = run_instance.fn(inst2, template)
    assert r1.results[0].records_out == 50
    assert r2.results[0].records_out == 200


def test_flow_runs() -> None:
    state = pipeline_template_flow(return_state=True)
    assert state.is_completed()
