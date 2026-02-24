"""Error Recovery.

Checkpoint-based stage recovery. The flow saves progress after each
successful stage. On re-run, it skips already-completed stages and
resumes from the last checkpoint.

Airflow equivalent: None (production resilience pattern).
Prefect approach:    JSON checkpoint file, stage skip logic, fail_on
                     parameter for testing.
"""

import datetime
import json
import tempfile
from pathlib import Path

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class Checkpoint(BaseModel):
    """A checkpoint for a single stage."""

    stage: str
    status: str  # completed, failed
    data: dict
    timestamp: str


class CheckpointStore(BaseModel):
    """Store of all checkpoints for a pipeline."""

    pipeline_id: str
    checkpoints: dict[str, Checkpoint] = {}


class RecoveryReport(BaseModel):
    """Report of a recovery-aware pipeline run."""

    stages_total: int
    stages_recovered: int
    stages_executed: int
    stages_failed: int
    final_status: str


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def load_checkpoints(path: Path) -> CheckpointStore:
    """Load checkpoints from disk.

    Args:
        path: Path to the checkpoint file.

    Returns:
        CheckpointStore (empty if file does not exist).
    """
    if path.exists():
        raw = json.loads(path.read_text())
        store = CheckpointStore(**raw)
        print(f"Loaded {len(store.checkpoints)} checkpoints")
        return store
    print("No checkpoints found, starting fresh")
    return CheckpointStore(pipeline_id="pipeline_079")


@task
def save_checkpoint(
    store: CheckpointStore,
    stage: str,
    status: str,
    data: dict,
    path: Path,
) -> CheckpointStore:
    """Save a checkpoint after a stage completes.

    Args:
        store: Current checkpoint store.
        stage: Stage name.
        status: Stage status (completed/failed).
        data: Stage output data.
        path: Path to save the checkpoint file.

    Returns:
        Updated CheckpointStore.
    """
    checkpoint = Checkpoint(
        stage=stage,
        status=status,
        data=data,
        timestamp=datetime.datetime.now(datetime.UTC).isoformat(),
    )
    store.checkpoints[stage] = checkpoint
    path.write_text(json.dumps(store.model_dump(), indent=2))
    return store


@task
def should_run_stage(store: CheckpointStore, stage: str) -> bool:
    """Check whether a stage should run based on checkpoints.

    Args:
        store: Current checkpoint store.
        stage: Stage name to check.

    Returns:
        True if the stage should run, False if already completed.
    """
    if stage in store.checkpoints and store.checkpoints[stage].status == "completed":
        print(f"Stage '{stage}' already completed, skipping")
        return False
    return True


@task
def execute_stage(stage_name: str, input_data: dict, fail_on: str | None = None) -> dict:
    """Execute a pipeline stage (simulated).

    Args:
        stage_name: Name of the stage.
        input_data: Input data for the stage.
        fail_on: If this matches stage_name, raise an error.

    Returns:
        Stage output dict.

    Raises:
        RuntimeError: If fail_on matches this stage.
    """
    if fail_on and stage_name == fail_on:
        raise RuntimeError(f"Simulated failure at stage '{stage_name}'")

    records = input_data.get("records", [])
    if stage_name == "extract":
        records = [{"id": i, "value": i * 10} for i in range(1, 6)]
    elif stage_name == "validate":
        records = [r for r in records if r.get("value", 0) >= 0]
    elif stage_name == "transform":
        records = [{**r, "value": r["value"] * 2} for r in records]
    elif stage_name == "load":
        pass  # records unchanged

    return {"records": records, "count": len(records)}


@task
def run_with_checkpoints(
    stages: list[str],
    store_path: Path,
    fail_on: str | None = None,
) -> RecoveryReport:
    """Run stages with checkpoint-based recovery.

    Args:
        stages: Ordered list of stage names.
        store_path: Path for checkpoint file.
        fail_on: Stage name to simulate failure on.

    Returns:
        RecoveryReport.
    """
    store = load_checkpoints.fn(store_path)
    recovered = 0
    executed = 0
    failed = 0
    context: dict = {}

    # Recover context from last completed stage
    for stage in stages:
        if stage in store.checkpoints and store.checkpoints[stage].status == "completed":
            context = store.checkpoints[stage].data

    for stage in stages:
        if not should_run_stage.fn(store, stage):
            recovered += 1
            continue

        try:
            result = execute_stage.fn(stage, context, fail_on)
            context = result
            store = save_checkpoint.fn(store, stage, "completed", result, store_path)
            executed += 1
            print(f"Stage '{stage}' completed")
        except RuntimeError as e:
            store = save_checkpoint.fn(store, stage, "failed", {"error": str(e)}, store_path)
            failed += 1
            print(f"Stage '{stage}' failed: {e}")
            break

    final_status = "completed" if failed == 0 else "failed"
    return RecoveryReport(
        stages_total=len(stages),
        stages_recovered=recovered,
        stages_executed=executed,
        stages_failed=failed,
        final_status=final_status,
    )


@task
def recovery_summary(report: RecoveryReport) -> str:
    """Generate a recovery summary.

    Args:
        report: Recovery report.

    Returns:
        Summary string.
    """
    return (
        f"Pipeline {report.final_status}: "
        f"{report.stages_executed} executed, {report.stages_recovered} recovered, "
        f"{report.stages_failed} failed out of {report.stages_total} stages"
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="data_engineering_error_recovery", log_prints=True)
def error_recovery_flow(
    work_dir: str | None = None,
    fail_on: str | None = None,
) -> RecoveryReport:
    """Run a pipeline with checkpoint-based error recovery.

    Args:
        work_dir: Working directory. Uses temp dir if not provided.
        fail_on: Stage to simulate failure on.

    Returns:
        RecoveryReport.
    """
    if work_dir is None:
        work_dir = tempfile.mkdtemp(prefix="error_recovery_")

    store_path = Path(work_dir) / "checkpoints.json"
    Path(work_dir).mkdir(parents=True, exist_ok=True)

    stages = ["extract", "validate", "transform", "load"]
    report = run_with_checkpoints(stages, store_path, fail_on)
    summary = recovery_summary(report)
    print(summary)
    return report


if __name__ == "__main__":
    error_recovery_flow()
