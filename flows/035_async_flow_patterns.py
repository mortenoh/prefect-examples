"""035 — Async Flow Patterns.

Demonstrate mixing sync and async tasks in an async flow, with an
async subflow for concurrent enrichment.

Airflow equivalent: Mix of standard and deferrable operators in a DAG.
Prefect approach:    Sync tasks called normally, async tasks awaited,
                     async subflow with asyncio.gather() for fan-out.
"""

import asyncio

from prefect import flow, task


@task
def sync_extract() -> list[str]:
    """Extract records synchronously.

    Returns:
        A list of raw record strings.
    """
    records = ["record-A", "record-B", "record-C", "record-D"]
    print(f"Extracted {len(records)} records (sync)")
    return records


@task
async def async_enrich(record: str) -> str:
    """Enrich a single record asynchronously.

    Args:
        record: The raw record string.

    Returns:
        An enriched record string.
    """
    await asyncio.sleep(0.05)  # Simulate async I/O
    enriched = f"enriched:{record}"
    print(f"Enriched: {record} -> {enriched}")
    return enriched


@task
def sync_load(records: list[str]) -> str:
    """Load enriched records synchronously.

    Args:
        records: List of enriched record strings.

    Returns:
        A load summary string.
    """
    msg = f"Loaded {len(records)} enriched records"
    print(msg)
    return msg


@flow(name="035_enrich_subflow", log_prints=True)
async def enrich_subflow(records: list[str]) -> list[str]:
    """Enrich all records concurrently using asyncio.gather.

    Args:
        records: List of raw record strings.

    Returns:
        List of enriched record strings.
    """
    enriched = await asyncio.gather(*[async_enrich(r) for r in records])
    print(f"Enrichment subflow complete: {len(enriched)} records")
    return list(enriched)


@flow(name="035_async_flow_patterns", log_prints=True)
async def async_flow_patterns_flow() -> None:
    """Mix sync extract → async enrich subflow → sync load."""
    raw = sync_extract()
    enriched = await enrich_subflow(raw)
    sync_load(enriched)


if __name__ == "__main__":
    asyncio.run(async_flow_patterns_flow())
