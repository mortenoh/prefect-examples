"""Nested Subflows.

Organize complex pipelines using nested subflows as logical groups.

Airflow equivalent: TaskGroups and nested groups (DAG 009).
Prefect approach:    @flow calling @flow for hierarchical grouping.
"""

from typing import Any

from dotenv import load_dotenv
from prefect import flow, task

# ---------------------------------------------------------------------------
# Extract group
# ---------------------------------------------------------------------------


@task
def fetch_source_a() -> list[dict[str, Any]]:
    """Fetch records from source A.

    Returns:
        A list of records.
    """
    records = [{"id": 1, "source": "A", "value": 10}]
    print(f"Fetched {len(records)} from source A")
    return records


@task
def fetch_source_b() -> list[dict[str, Any]]:
    """Fetch records from source B.

    Returns:
        A list of records.
    """
    records = [{"id": 2, "source": "B", "value": 20}]
    print(f"Fetched {len(records)} from source B")
    return records


@flow(name="patterns_extract_group", log_prints=True)
def extract_group() -> list[dict[str, Any]]:
    """Extract data from multiple sources.

    Returns:
        Combined list of records from all sources.
    """
    a = fetch_source_a()
    b = fetch_source_b()
    combined = a + b
    print(f"Extract group: {len(combined)} total records")
    return combined


# ---------------------------------------------------------------------------
# Transform group
# ---------------------------------------------------------------------------


@task
def clean_record(record: dict[str, Any]) -> dict[str, Any]:
    """Clean a single record.

    Args:
        record: A raw record dict.

    Returns:
        The cleaned record.
    """
    cleaned = {**record, "cleaned": True}
    print(f"Cleaned record {record['id']}")
    return cleaned


@task
def enrich_record(record: dict[str, Any]) -> dict[str, Any]:
    """Enrich a cleaned record with derived fields.

    Args:
        record: A cleaned record dict.

    Returns:
        The enriched record.
    """
    enriched = {**record, "enriched": True, "doubled_value": record["value"] * 2}
    print(f"Enriched record {record['id']}")
    return enriched


@flow(name="patterns_transform_group", log_prints=True)
def transform_group(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Clean and enrich all records.

    Args:
        records: Raw records to process.

    Returns:
        List of cleaned and enriched records.
    """
    results = []
    for record in records:
        cleaned = clean_record(record)
        enriched = enrich_record(cleaned)
        results.append(enriched)
    print(f"Transform group: {len(results)} records processed")
    return results


# ---------------------------------------------------------------------------
# Load group
# ---------------------------------------------------------------------------


@task
def write_to_target(records: list[dict[str, Any]]) -> str:
    """Write records to the target system.

    Args:
        records: The records to write.

    Returns:
        A summary string.
    """
    summary = f"Wrote {len(records)} records to target"
    print(summary)
    return summary


@task
def verify_load(summary: str) -> str:
    """Verify the load step succeeded.

    Args:
        summary: The load summary to verify.

    Returns:
        A verification message.
    """
    msg = f"Verified: {summary}"
    print(msg)
    return msg


@flow(name="patterns_load_group", log_prints=True)
def load_group(records: list[dict[str, Any]]) -> str:
    """Write records and verify the load.

    Args:
        records: Records to load.

    Returns:
        A verification message.
    """
    summary = write_to_target(records)
    result = verify_load(summary)
    return result


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


@flow(name="patterns_nested_subflows", log_prints=True)
def nested_subflows_flow() -> None:
    """Run the full pipeline using nested subflows for logical grouping."""
    raw = extract_group()
    transformed = transform_group(raw)
    result = load_group(transformed)
    print(f"Pipeline complete: {result}")


if __name__ == "__main__":
    load_dotenv()
    nested_subflows_flow()
