"""Pipeline Template Factory.

Reusable pipeline templates with ordered stage slots. Instantiate
the same template with different configurations to produce different
pipelines.

Airflow equivalent: None (Prefect-native pattern combining factory +
                    config-driven concepts).
Prefect approach:    Define templates with stage slots, instantiate with
                     overrides, execute, and compare results.
"""

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class StageTemplate(BaseModel):
    """Template for a pipeline stage."""

    name: str
    stage_type: str
    default_params: dict[str, str | int | float | bool]


class PipelineTemplate(BaseModel):
    """Reusable pipeline template."""

    template_name: str
    stages: list[StageTemplate]
    version: str


class StageResult(BaseModel):
    """Result of executing a single stage."""

    stage_name: str
    stage_type: str
    records_out: int
    params_used: dict[str, str | int | float | bool]


class TemplateInstance(BaseModel):
    """An instantiated pipeline from a template."""

    template_name: str
    overrides: dict[str, str | int | float | bool]
    results: list[StageResult]


class FactoryReport(BaseModel):
    """Summary factory report."""

    templates_used: list[str]
    instances_created: int
    results: list[TemplateInstance]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def create_template(name: str, stages: list[StageTemplate], version: str = "1.0") -> PipelineTemplate:
    """Create a pipeline template.

    Args:
        name: Template name.
        stages: Ordered stage definitions.
        version: Template version.

    Returns:
        PipelineTemplate.
    """
    return PipelineTemplate(template_name=name, stages=stages, version=version)


@task
def instantiate_template(
    template: PipelineTemplate,
    overrides: dict[str, str | int | float | bool],
) -> TemplateInstance:
    """Instantiate a template with configuration overrides.

    Args:
        template: Pipeline template.
        overrides: Parameter overrides.

    Returns:
        TemplateInstance (results empty, to be filled by execution).
    """
    return TemplateInstance(
        template_name=template.template_name,
        overrides=overrides,
        results=[],
    )


@task
def execute_stage(stage: StageTemplate, overrides: dict[str, str | int | float | bool]) -> StageResult:
    """Execute a single stage with merged parameters.

    Args:
        stage: Stage template.
        overrides: Parameter overrides.

    Returns:
        StageResult.
    """
    merged = {**stage.default_params, **overrides}

    # Simulate different stage types
    if stage.stage_type == "extract":
        count = int(merged.get("batch_size", 100))
    elif stage.stage_type == "validate":
        count = int(int(merged.get("batch_size", 100)) * float(merged.get("pass_rate", 0.9)))
    elif stage.stage_type == "transform" or stage.stage_type == "load":
        count = int(merged.get("batch_size", 100))
    else:
        count = 0

    return StageResult(
        stage_name=stage.name,
        stage_type=stage.stage_type,
        records_out=count,
        params_used=merged,
    )


@task
def run_instance(
    instance: TemplateInstance,
    template: PipelineTemplate,
) -> TemplateInstance:
    """Execute all stages in a template instance.

    Args:
        instance: Template instance.
        template: Original template with stage definitions.

    Returns:
        TemplateInstance with results populated.
    """
    results: list[StageResult] = []
    for stage in template.stages:
        result = execute_stage.fn(stage, instance.overrides)
        results.append(result)
    return instance.model_copy(update={"results": results})


@task
def factory_report(instances: list[TemplateInstance]) -> FactoryReport:
    """Build the final factory report.

    Args:
        instances: Executed template instances.

    Returns:
        FactoryReport.
    """
    templates_used = list({i.template_name for i in instances})
    return FactoryReport(
        templates_used=sorted(templates_used),
        instances_created=len(instances),
        results=instances,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="analytics_pipeline_template", log_prints=True)
def pipeline_template_flow() -> FactoryReport:
    """Run the pipeline template factory.

    Returns:
        FactoryReport.
    """
    # Define templates
    etl_basic = create_template(
        "etl_basic",
        [
            StageTemplate(name="extract", stage_type="extract", default_params={"batch_size": 100}),
            StageTemplate(name="load", stage_type="load", default_params={"batch_size": 100}),
        ],
    )

    etl_full = create_template(
        "etl_full",
        [
            StageTemplate(name="extract", stage_type="extract", default_params={"batch_size": 200}),
            StageTemplate(
                name="validate", stage_type="validate", default_params={"batch_size": 200, "pass_rate": 0.95}
            ),
            StageTemplate(name="transform", stage_type="transform", default_params={"batch_size": 200}),
            StageTemplate(name="load", stage_type="load", default_params={"batch_size": 200}),
        ],
    )

    # Instantiate with different configs
    basic_small = instantiate_template(etl_basic, {"batch_size": 50})
    basic_large = instantiate_template(etl_basic, {"batch_size": 500})
    full_default = instantiate_template(etl_full, {})
    full_custom = instantiate_template(etl_full, {"batch_size": 1000, "pass_rate": 0.8})

    # Execute
    r1 = run_instance(basic_small, etl_basic)
    r2 = run_instance(basic_large, etl_basic)
    r3 = run_instance(full_default, etl_full)
    r4 = run_instance(full_custom, etl_full)

    report = factory_report([r1, r2, r3, r4])
    print(f"Factory: {report.instances_created} instances from {len(report.templates_used)} templates")
    return report


if __name__ == "__main__":
    pipeline_template_flow()
