"""Config-Driven Pipeline.

Pipeline behaviour controlled by a configuration dict: stage selection,
parameter overrides, and conditional execution. Different configs produce
different pipeline runs through the same flow.

Airflow equivalent: API-triggered scheduling with config payload (DAG 109).
Prefect approach:    Pydantic config models, stage dispatcher @task.
"""

from typing import Any

from dotenv import load_dotenv
from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class StageConfig(BaseModel):
    """Configuration for a single pipeline stage."""

    name: str
    enabled: bool = True
    task_type: str = "default"
    params: dict[str, Any] = {}


class PipelineConfig(BaseModel):
    """Configuration for the entire pipeline."""

    name: str
    stages: list[StageConfig]
    fail_fast: bool = True


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
def execute_extract(params: dict[str, Any]) -> dict[str, Any]:
    """Simulate an extract stage.

    Args:
        params: Stage parameters.

    Returns:
        Extract result.
    """
    count = params.get("count", 10)
    records = [{"id": i, "value": i * 10} for i in range(1, count + 1)]
    return {"stage": "extract", "records": records, "count": count}


@task
def execute_validate(params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """Simulate a validate stage.

    Args:
        params: Stage parameters.
        context: Data from previous stages.

    Returns:
        Validation result.
    """
    records = context.get("records", [])
    min_val = params.get("min_value", 0)
    valid = [r for r in records if r.get("value", 0) >= min_val]
    return {"stage": "validate", "records": valid, "count": len(valid), "rejected": len(records) - len(valid)}


@task
def execute_transform(params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """Simulate a transform stage.

    Args:
        params: Stage parameters.
        context: Data from previous stages.

    Returns:
        Transform result.
    """
    records = context.get("records", [])
    multiplier = params.get("multiplier", 1.0)
    transformed = [{**r, "value": r["value"] * multiplier} for r in records]
    return {"stage": "transform", "records": transformed, "count": len(transformed)}


@task
def execute_load(params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """Simulate a load stage.

    Args:
        params: Stage parameters.
        context: Data from previous stages.

    Returns:
        Load result.
    """
    records = context.get("records", [])
    target = params.get("target", "default_table")
    return {"stage": "load", "target": target, "count": len(records), "records": records}


@task
def dispatch_stage(stage: StageConfig, context: dict[str, Any]) -> StageResult:
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

    result: dict[str, Any] = (
        handler.fn(stage.params) if stage.task_type == "extract" else handler.fn(stage.params, context)  # type: ignore[attr-defined]
    )

    return StageResult(
        stage_name=stage.name,
        status="completed",
        records_in=context.get("count", 0),
        records_out=result.get("count", 0),
        details=f"Processed by {stage.task_type}",
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="data_engineering_config_driven_pipeline", log_prints=True)
def config_driven_pipeline_flow(raw_config: dict[str, Any] | None = None) -> PipelineResult:
    """Execute a pipeline driven by configuration.

    Args:
        raw_config: Pipeline config dict. Uses default if None.

    Returns:
        PipelineResult.
    """
    if raw_config is None:
        raw_config = {
            "name": "default_pipeline",
            "stages": [
                {"name": "extract", "task_type": "extract", "params": {"count": 20}},
                {"name": "validate", "task_type": "validate", "params": {"min_value": 50}},
                {"name": "transform", "task_type": "transform", "params": {"multiplier": 1.5}},
                {"name": "load", "task_type": "load", "params": {"target": "output_table"}},
            ],
        }

    config = parse_config(raw_config)

    results: list[StageResult] = []
    context: dict[str, Any] = {}
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
