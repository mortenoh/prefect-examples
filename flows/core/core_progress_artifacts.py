"""Progress Artifacts.

Demonstrate progress artifacts for tracking long-running operations in the UI.

Airflow equivalent: Custom XCom updates or external progress-tracking systems.
Prefect approach:    create_progress_artifact() and update_progress_artifact()
                     to show live completion percentage in the Prefect UI.
"""

import time
from typing import Any

from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact, create_progress_artifact, update_progress_artifact


@task
def process_batch(items: list[str], label: str) -> dict[str, Any]:
    """Process a batch of items with a progress artifact.

    Creates a progress artifact at 0%, updates it as each item completes,
    and finishes at 100%.

    Args:
        items: Work items to process.
        label: Label for the batch (used in artifact key).

    Returns:
        Summary dict with batch label, item count, and duration.
    """
    start = time.monotonic()
    artifact_id = create_progress_artifact(
        progress=0.0,
        key=f"batch-progress-{label}",
        description=f"Processing batch: {label}",
    )
    for i, item in enumerate(items, 1):
        time.sleep(0.05)
        progress = (i / len(items)) * 100
        print(f"[{label}] Processed {item} ({progress:.0f}%)")
        if artifact_id:
            update_progress_artifact(artifact_id=artifact_id, progress=progress)

    duration = time.monotonic() - start
    return {"label": label, "item_count": len(items), "duration": round(duration, 2)}


@task
def publish_summary(batch_results: list[dict[str, Any]]) -> str:
    """Publish a markdown summary of all completed batches.

    Shows that progress artifacts compose with other artifact types.

    Args:
        batch_results: List of summary dicts from process_batch.

    Returns:
        The markdown content that was published.
    """
    rows = "\n".join(f"| {r['label']} | {r['item_count']} | {r['duration']}s |" for r in batch_results)
    total_items = sum(r["item_count"] for r in batch_results)

    markdown = f"""# Batch Processing Summary

**Batches completed:** {len(batch_results)}
**Total items processed:** {total_items}

| Batch | Items | Duration |
|-------|-------|----------|
{rows}
"""

    create_markdown_artifact(
        key="batch-summary",
        markdown=markdown,
        description="Summary of all processed batches",
    )
    print(f"Published batch summary artifact ({len(batch_results)} batches)")
    return markdown


@flow(name="core_progress_artifacts", log_prints=True)
def progress_artifacts_flow() -> None:
    """Process multiple batches with per-task progress tracking."""
    batches = {
        "alpha": ["a1", "a2", "a3", "a4"],
        "beta": ["b1", "b2", "b3"],
        "gamma": ["g1", "g2", "g3", "g4", "g5"],
    }
    results = []
    for label, items in batches.items():
        result = process_batch(items, label)
        results.append(result)

    publish_summary(results)


if __name__ == "__main__":
    load_dotenv()
    progress_artifacts_flow()
