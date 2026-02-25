"""Markdown Artifacts.

Demonstrate creating markdown artifacts for rich reporting in the Prefect UI.

Airflow equivalent: Custom HTML in XCom or external reporting tools.
Prefect approach:    create_markdown_artifact() to publish formatted reports
                     visible in the Prefect UI.
"""

import datetime
from typing import Any

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact


@task
def generate_data() -> list[dict[str, Any]]:
    """Generate sample data for the report.

    Returns:
        A list of data records.
    """
    records = [
        {"name": "Alice", "department": "Engineering", "score": 92},
        {"name": "Bob", "department": "Sales", "score": 87},
        {"name": "Charlie", "department": "Engineering", "score": 95},
        {"name": "Diana", "department": "Marketing", "score": 78},
        {"name": "Eve", "department": "Sales", "score": 91},
    ]
    print(f"Generated {len(records)} records")
    return records


@task
def publish_report(results: list[dict[str, Any]]) -> str:
    """Publish a markdown report as a Prefect artifact.

    The artifact is viewable in the Prefect UI under the Artifacts tab.
    Without a Prefect server, create_markdown_artifact() silently no-ops.

    Args:
        results: Data records to include in the report.

    Returns:
        The markdown content that was published.
    """
    timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M UTC")

    rows = "\n".join(f"| {r['name']} | {r['department']} | {r['score']} |" for r in results)
    avg_score = sum(r["score"] for r in results) / len(results) if results else 0

    markdown = f"""# Performance Report

**Generated:** {timestamp}
**Records:** {len(results)}
**Average Score:** {avg_score:.1f}

| Name | Department | Score |
|------|------------|-------|
{rows}

## Summary

Top performer: {max(results, key=lambda r: r["score"])["name"]}
"""

    create_markdown_artifact(
        key="performance-report",
        markdown=markdown,
        description="Weekly performance report",
    )
    print(f"Published markdown artifact ({len(markdown)} chars)")
    return markdown


@flow(name="core_markdown_artifacts", log_prints=True)
def markdown_artifacts_flow() -> None:
    """Generate data and publish a markdown artifact report."""
    data = generate_data()
    publish_report(data)


if __name__ == "__main__":
    markdown_artifacts_flow()
