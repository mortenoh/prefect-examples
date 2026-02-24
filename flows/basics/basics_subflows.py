"""Subflows.

Compose larger pipelines from smaller, reusable flows.

Airflow equivalent: TaskGroup / SubDagOperator.
Prefect approach:    @flow calling @flow — nested flow runs in UI.
"""

from prefect import flow, task


@task
def build_records() -> list[dict]:
    """Return a handful of sample records."""
    records = [
        {"id": 1, "value": "alpha"},
        {"id": 2, "value": "beta"},
        {"id": 3, "value": "gamma"},
    ]
    print(f"Built {len(records)} records")
    return records


@flow(name="basics_extract", log_prints=True)
def extract_flow() -> list[dict]:
    """Extract raw records from a data source."""
    return build_records()


@flow(name="basics_transform", log_prints=True)
def transform_flow(raw: list[dict]) -> list[dict]:
    """Add a 'processed' flag to each record."""
    transformed = [{**record, "processed": True} for record in raw]
    print(f"Transformed {len(transformed)} records")
    return transformed


@flow(name="basics_load", log_prints=True)
def load_flow(data: list[dict]) -> str:
    """Load data and return a summary string."""
    summary = f"Loaded {len(data)} records: {[r['id'] for r in data]}"
    print(summary)
    return summary


@flow(name="basics_subflows", log_prints=True)
def pipeline_flow() -> None:
    """Run extract → transform → load as chained subflows."""
    raw = extract_flow()
    transformed = transform_flow(raw)
    load_flow(transformed)


if __name__ == "__main__":
    pipeline_flow()
