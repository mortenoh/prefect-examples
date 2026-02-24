"""097 -- Data Lineage Tracking.

Track data provenance through pipeline stages. Each transformation
records input hash, output hash, operation name, and timestamp to
build a lineage graph.

Airflow equivalent: None (Prefect-native observability pattern).
Prefect approach:    Ingest -> transform (filter, enrich, dedup) -> build
                     lineage graph from entries -> report.
"""

import datetime
import hashlib

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class DataRecord(BaseModel):
    """A generic data record flowing through the pipeline."""

    id: int
    name: str
    value: int
    quality_tier: str = ""


class LineageEntry(BaseModel):
    """A single lineage tracking entry."""

    stage: str
    operation: str
    input_hash: str
    output_hash: str
    record_count: int
    timestamp: str


class LineageGraph(BaseModel):
    """Graph of lineage entries through the pipeline."""

    entries: list[LineageEntry]
    stages: list[str]
    total_records_processed: int


class LineageReport(BaseModel):
    """Summary lineage report."""

    graph: LineageGraph
    stage_count: int
    data_modifications: int


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def compute_data_hash(records: list[DataRecord]) -> str:
    """Compute a deterministic hash of a dataset.

    Args:
        records: List of records.

    Returns:
        Hex digest string.
    """
    raw = str(sorted(str(r.model_dump()) for r in records))
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


@task
def record_lineage(
    stage: str,
    operation: str,
    input_hash: str,
    output_hash: str,
    count: int,
) -> LineageEntry:
    """Record a lineage entry for a pipeline stage.

    Args:
        stage: Stage name.
        operation: Operation description.
        input_hash: Hash of input data.
        output_hash: Hash of output data.
        count: Record count after operation.

    Returns:
        LineageEntry.
    """
    return LineageEntry(
        stage=stage,
        operation=operation,
        input_hash=input_hash,
        output_hash=output_hash,
        record_count=count,
        timestamp=datetime.datetime.now(datetime.UTC).isoformat(),
    )


@task
def ingest_with_lineage(data: list[DataRecord]) -> tuple[list[DataRecord], LineageEntry]:
    """Ingest data and record lineage.

    Args:
        data: Raw input data.

    Returns:
        Tuple of (data, lineage entry).
    """
    input_hash = compute_data_hash.fn(data)
    output_hash = input_hash  # Ingest doesn't modify data
    entry = record_lineage.fn("ingest", "raw_ingest", input_hash, output_hash, len(data))
    print(f"Ingested {len(data)} records")
    return data, entry


@task
def transform_filter(records: list[DataRecord], min_value: float) -> tuple[list[DataRecord], LineageEntry]:
    """Filter records and record lineage.

    Args:
        records: Input records.
        min_value: Minimum value threshold.

    Returns:
        Tuple of (filtered records, lineage entry).
    """
    input_hash = compute_data_hash.fn(records)
    filtered = [r for r in records if r.value >= min_value]
    output_hash = compute_data_hash.fn(filtered)
    entry = record_lineage.fn("transform", "filter_by_value", input_hash, output_hash, len(filtered))
    print(f"Filtered: {len(records)} -> {len(filtered)}")
    return filtered, entry


@task
def transform_enrich(records: list[DataRecord]) -> tuple[list[DataRecord], LineageEntry]:
    """Enrich records and record lineage.

    Args:
        records: Input records.

    Returns:
        Tuple of (enriched records, lineage entry).
    """
    input_hash = compute_data_hash.fn(records)
    enriched = [r.model_copy(update={"quality_tier": "high" if r.value > 50 else "standard"}) for r in records]
    output_hash = compute_data_hash.fn(enriched)
    entry = record_lineage.fn("transform", "enrich", input_hash, output_hash, len(enriched))
    print(f"Enriched {len(enriched)} records")
    return enriched, entry


@task
def transform_dedup(records: list[DataRecord], key_field: str) -> tuple[list[DataRecord], LineageEntry]:
    """Deduplicate records and record lineage.

    Args:
        records: Input records.
        key_field: Field to deduplicate on.

    Returns:
        Tuple of (deduped records, lineage entry).
    """
    input_hash = compute_data_hash.fn(records)
    seen: set[str] = set()
    unique: list[DataRecord] = []
    for r in records:
        key = str(getattr(r, key_field, ""))
        if key not in seen:
            seen.add(key)
            unique.append(r)
    output_hash = compute_data_hash.fn(unique)
    entry = record_lineage.fn("transform", "deduplicate", input_hash, output_hash, len(unique))
    print(f"Deduped: {len(records)} -> {len(unique)}")
    return unique, entry


@task
def build_lineage_graph(entries: list[LineageEntry]) -> LineageGraph:
    """Build a lineage graph from entries.

    Args:
        entries: List of lineage entries.

    Returns:
        LineageGraph.
    """
    stages = [e.stage for e in entries]
    total = sum(e.record_count for e in entries)
    return LineageGraph(entries=entries, stages=stages, total_records_processed=total)


@task
def lineage_report(graph: LineageGraph) -> LineageReport:
    """Build the final lineage report.

    Args:
        graph: Lineage graph.

    Returns:
        LineageReport.
    """
    modifications = sum(1 for e in graph.entries if e.input_hash != e.output_hash)
    return LineageReport(
        graph=graph,
        stage_count=len(graph.entries),
        data_modifications=modifications,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="097_data_lineage", log_prints=True)
def data_lineage_flow() -> LineageReport:
    """Run the data lineage tracking pipeline.

    Returns:
        LineageReport.
    """
    raw_data = [
        DataRecord(id=1, name="alpha", value=80),
        DataRecord(id=2, name="beta", value=30),
        DataRecord(id=3, name="gamma", value=60),
        DataRecord(id=4, name="delta", value=10),
        DataRecord(id=5, name="epsilon", value=90),
        DataRecord(id=1, name="alpha", value=80),  # duplicate
    ]

    data, entry_ingest = ingest_with_lineage(raw_data)
    data, entry_filter = transform_filter(data, min_value=20)
    data, entry_enrich = transform_enrich(data)
    data, entry_dedup = transform_dedup(data, "id")

    graph = build_lineage_graph([entry_ingest, entry_filter, entry_enrich, entry_dedup])
    report = lineage_report(graph)
    print(f"Lineage: {report.stage_count} stages, {report.data_modifications} modifications")
    return report


if __name__ == "__main__":
    data_lineage_flow()
