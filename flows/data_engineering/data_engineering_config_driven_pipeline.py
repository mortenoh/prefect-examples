"""Config-Driven Pipeline.

Pipeline behaviour controlled by a configuration dict: stage selection,
parameter overrides, and conditional execution. Different configs produce
different pipeline runs through the same flow.

Airflow equivalent: API-triggered scheduling with config payload (DAG 109).
Prefect approach:    Pydantic config models, stage dispatcher @task.
"""

from typing import Any, Literal

from dotenv import load_dotenv
from prefect import flow, task
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class StageConfig(BaseModel):
    """Configuration for a single pipeline stage."""

    name: str = Field(description="Stage identifier")
    enabled: bool = Field(default=True, description="Whether to execute this stage")
    task_type: Literal["extract", "validate", "transform", "load"] = Field(description="Handler type for this stage")
    params: dict[str, Any] = Field(default={}, description="Handler-specific parameters")


class PipelineConfig(BaseModel):
    """Configuration for the entire pipeline."""

    name: str = Field(description="Pipeline identifier")
    stages: list[StageConfig] = Field(description="Ordered list of stages to execute")
    fail_fast: bool = Field(default=True, description="Stop on first stage failure")


class StageResult(BaseModel):
    """Result from executing a single stage."""

    stage_name: str
    status: str
    records_in: int = 0
    records_out: int = 0
    details: str = ""


class PipelineResult(BaseModel):
    """Result of the entire pipeline."""

    pipeline_name: str
    stages_executed: int
    stages_skipped: int
    results: list[StageResult]


class StageContext(BaseModel):
    """Inter-stage context passed between pipeline stages."""

    stage: str = ""
    records: list[dict[str, Any]] = []
    count: int = 0
    target: str = ""
    rejected: int = 0


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def parse_config(raw: dict[str, Any]) -> PipelineConfig:
    """Parse a raw config dict into a PipelineConfig.

    Args:
        raw: Raw configuration dict.

    Returns:
        Validated PipelineConfig.
    """
    config = PipelineConfig(**raw)
    print(f"Parsed config '{config.name}' with {len(config.stages)} stages")
    return config


@task
def execute_extract(params: dict[str, Any]) -> StageContext:
    """Simulate an extract stage.

    Args:
        params: Stage parameters.

    Returns:
        StageContext with extracted records.
    """
    count = params.get("count", 10)
    records = [{"id": i, "value": i * 10} for i in range(1, count + 1)]
    return StageContext(stage="extract", records=records, count=count)


@task
def execute_validate(params: dict[str, Any], context: StageContext) -> StageContext:
    """Simulate a validate stage.

    Args:
        params: Stage parameters.
        context: Data from previous stages.

    Returns:
        StageContext with validated records.
    """
    min_val = params.get("min_value", 0)
    valid = [r for r in context.records if r.get("value", 0) >= min_val]
    return StageContext(
        stage="validate",
        records=valid,
        count=len(valid),
        rejected=len(context.records) - len(valid),
    )


@task
def execute_transform(params: dict[str, Any], context: StageContext) -> StageContext:
    """Simulate a transform stage.

    Args:
        params: Stage parameters.
        context: Data from previous stages.

    Returns:
        StageContext with transformed records.
    """
    multiplier = params.get("multiplier", 1.0)
    transformed = [{**r, "value": r["value"] * multiplier} for r in context.records]
    return StageContext(stage="transform", records=transformed, count=len(transformed))


@task
def execute_load(params: dict[str, Any], context: StageContext) -> StageContext:
    """Simulate a load stage.

    Args:
        params: Stage parameters.
        context: Data from previous stages.

    Returns:
        StageContext with load metadata.
    """
    target = params.get("target", "default_table")
    return StageContext(stage="load", target=target, count=len(context.records), records=context.records)


@task
def dispatch_stage(stage: StageConfig, context: StageContext) -> StageResult:
    """Dispatch a stage to its handler based on task_type.

    Args:
        stage: Stage configuration.
        context: Pipeline context with data from previous stages.

    Returns:
        StageResult.
    """
    handlers = {
        "extract": execute_extract,
        "validate": execute_validate,
        "transform": execute_transform,
        "load": execute_load,
    }
    handler = handlers.get(stage.task_type)
    if handler is None:
        return StageResult(stage_name=stage.name, status="error", details=f"Unknown task type: {stage.task_type}")

    result: StageContext = (
        handler.fn(stage.params) if stage.task_type == "extract" else handler.fn(stage.params, context)  # type: ignore[attr-defined]
    )

    return StageResult(
        stage_name=stage.name,
        status="completed",
        records_in=context.count,
        records_out=result.count,
        details=f"Processed by {stage.task_type}",
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="data_engineering_config_driven_pipeline", log_prints=True)
def config_driven_pipeline_flow(config: PipelineConfig | None = None) -> PipelineResult:
    """Execute a pipeline driven by configuration.

    Args:
        config: Pipeline configuration. Uses default if None.

    Returns:
        PipelineResult.
    """
    if config is None:
        config = PipelineConfig(
            name="default_pipeline",
            stages=[
                StageConfig(name="extract", task_type="extract", params={"count": 20}),
                StageConfig(name="validate", task_type="validate", params={"min_value": 50}),
                StageConfig(name="transform", task_type="transform", params={"multiplier": 1.5}),
                StageConfig(name="load", task_type="load", params={"target": "output_table"}),
            ],
        )

    results: list[StageResult] = []
    context = StageContext()
    skipped = 0

    for stage in config.stages:
        if not stage.enabled:
            print(f"Skipping disabled stage: {stage.name}")
            skipped += 1
            continue

        result = dispatch_stage(stage, context)
        results.append(result)

        # Update context for next stage
        if stage.task_type == "extract":
            context = execute_extract.fn(stage.params)
        elif stage.task_type == "validate":
            context = execute_validate.fn(stage.params, context)
        elif stage.task_type == "transform":
            context = execute_transform.fn(stage.params, context)
        elif stage.task_type == "load":
            context = execute_load.fn(stage.params, context)

    pipeline_result = PipelineResult(
        pipeline_name=config.name,
        stages_executed=len(results),
        stages_skipped=skipped,
        results=results,
    )
    print(f"Pipeline '{config.name}': {pipeline_result.stages_executed} executed, {skipped} skipped")
    return pipeline_result


if __name__ == "__main__":
    load_dotenv()
    config_driven_pipeline_flow()
