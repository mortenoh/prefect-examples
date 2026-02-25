"""Data Profiling.

Statistical data profiling using the stdlib statistics module. Profiles each
column by type (numeric vs string) and computes descriptive statistics.

Airflow equivalent: Consolidated quality dashboard (DAG 072).
Prefect approach:    Column-level profiling with @task dispatchers, Pydantic
                     models for profile structure.
"""

import statistics
from typing import Any

from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class ColumnProfile(BaseModel):
    """Profile of a single column."""

    name: str
    dtype: str
    count: int
    null_count: int
    null_rate: float
    # Numeric stats (optional)
    mean: float | None = None
    stdev: float | None = None
    min_val: float | None = None
    max_val: float | None = None
    median: float | None = None
    # String stats (optional)
    min_length: int | None = None
    max_length: int | None = None
    unique_count: int | None = None


class DatasetProfile(BaseModel):
    """Profile of an entire dataset."""

    name: str
    row_count: int
    column_count: int
    columns: list[ColumnProfile]
    completeness_score: float


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def generate_dataset(name: str, rows: int = 50) -> list[dict[str, Any]]:
    """Generate a sample dataset for profiling.

    Args:
        name: Dataset name (used for seeding).
        rows: Number of rows.

    Returns:
        List of record dicts.
    """
    data = []
    for i in range(1, rows + 1):
        record: dict[str, Any] = {
            "id": i,
            "name": f"item_{i}" if i % 8 != 0 else None,
            "value": float(i * 7 % 100) if i % 12 != 0 else None,
            "category": ["alpha", "beta", "gamma", "delta"][i % 4],
            "score": round(50.0 + (i * 3.14) % 50, 2),
        }
        data.append(record)
    print(f"Generated dataset '{name}' with {rows} rows")
    return data


@task
def infer_column_type(values: list[Any]) -> str:
    """Infer the type of a column from its non-null values.

    Args:
        values: List of values (may contain None).

    Returns:
        Type string: "numeric", "string", or "empty".
    """
    non_null = [v for v in values if v is not None]
    if not non_null:
        return "empty"
    if all(isinstance(v, (int, float)) for v in non_null):
        return "numeric"
    return "string"


@task
def profile_numeric_column(name: str, values: list[Any]) -> ColumnProfile:
    """Profile a numeric column.

    Args:
        name: Column name.
        values: All values (may contain None).

    Returns:
        ColumnProfile with numeric statistics.
    """
    non_null = [float(v) for v in values if v is not None]
    null_count = len(values) - len(non_null)
    null_rate = null_count / len(values) if values else 0.0

    profile = ColumnProfile(
        name=name,
        dtype="numeric",
        count=len(values),
        null_count=null_count,
        null_rate=round(null_rate, 4),
    )
    if len(non_null) >= 2:
        profile.mean = round(statistics.mean(non_null), 4)
        profile.stdev = round(statistics.stdev(non_null), 4)
        profile.min_val = min(non_null)
        profile.max_val = max(non_null)
        profile.median = round(statistics.median(non_null), 4)
    elif len(non_null) == 1:
        profile.mean = non_null[0]
        profile.stdev = 0.0
        profile.min_val = non_null[0]
        profile.max_val = non_null[0]
        profile.median = non_null[0]
    return profile


@task
def profile_string_column(name: str, values: list[Any]) -> ColumnProfile:
    """Profile a string column.

    Args:
        name: Column name.
        values: All values (may contain None).

    Returns:
        ColumnProfile with string statistics.
    """
    non_null = [str(v) for v in values if v is not None]
    null_count = len(values) - len(non_null)
    null_rate = null_count / len(values) if values else 0.0
    lengths = [len(s) for s in non_null]

    return ColumnProfile(
        name=name,
        dtype="string",
        count=len(values),
        null_count=null_count,
        null_rate=round(null_rate, 4),
        min_length=min(lengths) if lengths else None,
        max_length=max(lengths) if lengths else None,
        unique_count=len(set(non_null)),
    )


@task
def profile_column(name: str, values: list[Any]) -> ColumnProfile:
    """Profile a column by dispatching to the appropriate profiler.

    Args:
        name: Column name.
        values: All values.

    Returns:
        ColumnProfile.
    """
    dtype = infer_column_type.fn(values)
    if dtype == "numeric":
        return profile_numeric_column.fn(name, values)
    return profile_string_column.fn(name, values)


@task
def profile_dataset(name: str, data: list[dict[str, Any]]) -> DatasetProfile:
    """Profile an entire dataset.

    Args:
        name: Dataset name.
        data: List of record dicts.

    Returns:
        DatasetProfile with column-level profiles.
    """
    if not data:
        return DatasetProfile(name=name, row_count=0, column_count=0, columns=[], completeness_score=0.0)

    columns = list(data[0].keys())
    profiles = []
    total_cells = 0
    null_cells = 0

    for col in columns:
        values = [row.get(col) for row in data]
        total_cells += len(values)
        null_cells += sum(1 for v in values if v is None)
        profiles.append(profile_column.fn(col, values))

    completeness = 1.0 - (null_cells / total_cells) if total_cells > 0 else 0.0

    return DatasetProfile(
        name=name,
        row_count=len(data),
        column_count=len(columns),
        columns=profiles,
        completeness_score=round(completeness, 4),
    )


@task
def publish_profile(profile: DatasetProfile) -> str:
    """Publish dataset profile as a markdown artifact.

    Args:
        profile: The dataset profile.

    Returns:
        Markdown string.
    """
    lines = [
        f"# Dataset Profile: {profile.name}",
        "",
        f"**Rows:** {profile.row_count} | **Columns:** {profile.column_count} | "
        f"**Completeness:** {profile.completeness_score:.1%}",
        "",
        "| Column | Type | Count | Nulls | Null Rate | Stats |",
        "|---|---|---|---|---|---|",
    ]
    for col in profile.columns:
        if col.dtype == "numeric":
            stats = f"mean={col.mean}, stdev={col.stdev}, range=[{col.min_val}, {col.max_val}]"
        elif col.unique_count is not None:
            stats = f"unique={col.unique_count}, len=[{col.min_length}, {col.max_length}]"
        else:
            stats = ""
        lines.append(f"| {col.name} | {col.dtype} | {col.count} | {col.null_count} | {col.null_rate:.1%} | {stats} |")

    markdown = "\n".join(lines)
    create_markdown_artifact(key="data-profile", markdown=markdown, description="Dataset profile")
    return markdown


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="data_engineering_data_profiling", log_prints=True)
def data_profiling_flow(dataset_name: str = "sample", rows: int = 50) -> DatasetProfile:
    """Profile a generated dataset and publish results.

    Args:
        dataset_name: Name for the dataset.
        rows: Number of rows to generate.

    Returns:
        DatasetProfile.
    """
    data = generate_dataset(dataset_name, rows=rows)
    profile = profile_dataset(dataset_name, data)
    publish_profile(profile)
    print(f"Profiled '{dataset_name}': {profile.row_count} rows, {profile.column_count} columns")
    return profile


if __name__ == "__main__":
    load_dotenv()
    data_profiling_flow()
