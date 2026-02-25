"""API Pagination.

Paginated API consumption with chunked parallel processing. Simulates
fetching pages from an API endpoint and processing records in chunks.

Airflow equivalent: Chunked API fetching (DAG 094).
Prefect approach:    Simulated pagination, itertools-style chunking,
                     .map() for parallel chunk processing.
"""

from typing import Any

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class PageResponse(BaseModel):
    """Response from a single API page."""

    page: int
    records: list[dict[str, Any]]
    total_pages: int
    has_next: bool


class PaginationResult(BaseModel):
    """Result of paginated processing."""

    total_records: int
    total_pages: int
    chunks_processed: int
    records_processed: int


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def simulate_api_page(page: int, page_size: int = 10, total_records: int = 45) -> PageResponse:
    """Simulate fetching a single page from an API.

    Args:
        page: Page number (1-based).
        page_size: Records per page.
        total_records: Total records in the simulated API.

    Returns:
        PageResponse with records and pagination metadata.
    """
    total_pages = (total_records + page_size - 1) // page_size
    start = (page - 1) * page_size
    end = min(start + page_size, total_records)
    records = [
        {"id": i + 1, "value": round((i + 1) * 2.5, 1), "category": ["A", "B", "C"][i % 3]} for i in range(start, end)
    ]
    has_next = page < total_pages
    print(f"Fetched page {page}/{total_pages}: {len(records)} records")
    return PageResponse(page=page, records=records, total_pages=total_pages, has_next=has_next)


@task
def fetch_all_pages(page_size: int = 10, total_records: int = 45) -> list[PageResponse]:
    """Fetch all pages from the simulated API.

    Args:
        page_size: Records per page.
        total_records: Total records available.

    Returns:
        List of all PageResponse objects.
    """
    pages = []
    page = 1
    while True:
        response = simulate_api_page.fn(page, page_size, total_records)
        pages.append(response)
        if not response.has_next:
            break
        page += 1
    print(f"Fetched {len(pages)} pages, {sum(len(p.records) for p in pages)} total records")
    return pages


@task
def chunk_records(records: list[dict[str, Any]], chunk_size: int = 15) -> list[list[dict[str, Any]]]:
    """Split records into chunks for parallel processing.

    Args:
        records: All records to chunk.
        chunk_size: Maximum records per chunk.

    Returns:
        List of record chunks.
    """
    chunks = []
    for i in range(0, len(records), chunk_size):
        chunks.append(records[i : i + chunk_size])
    print(f"Split {len(records)} records into {len(chunks)} chunks")
    return chunks


@task
def process_chunk(records: list[dict[str, Any]], chunk_id: int) -> dict[str, Any]:
    """Process a single chunk of records.

    Args:
        records: Records in this chunk.
        chunk_id: Identifier for this chunk.

    Returns:
        Dict with chunk processing summary.
    """
    total_value = sum(r.get("value", 0) for r in records)
    categories: dict[str, int] = {}
    for r in records:
        cat = r.get("category", "unknown")
        categories[cat] = categories.get(cat, 0) + 1
    return {
        "chunk_id": chunk_id,
        "record_count": len(records),
        "total_value": round(total_value, 1),
        "categories": categories,
    }


@task
def combine_chunk_results(results: list[dict[str, Any]], total_pages: int) -> PaginationResult:
    """Combine results from all processed chunks.

    Args:
        results: List of chunk processing results.
        total_pages: Total pages fetched.

    Returns:
        PaginationResult summary.
    """
    total_records = sum(r["record_count"] for r in results)
    return PaginationResult(
        total_records=total_records,
        total_pages=total_pages,
        chunks_processed=len(results),
        records_processed=total_records,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="data_engineering_api_pagination", log_prints=True)
def api_pagination_flow(
    page_size: int = 10,
    total_records: int = 45,
    chunk_size: int = 15,
) -> PaginationResult:
    """Fetch paginated API data and process in parallel chunks.

    Args:
        page_size: Records per API page.
        total_records: Total simulated records.
        chunk_size: Records per processing chunk.

    Returns:
        PaginationResult.
    """
    # Fetch all pages
    pages = fetch_all_pages(page_size, total_records)

    # Flatten records
    all_records = []
    for page in pages:
        all_records.extend(page.records)

    # Chunk and process
    chunks = chunk_records(all_records, chunk_size)
    chunk_ids = list(range(len(chunks)))
    futures = process_chunk.map(chunks, chunk_ids)
    results = [f.result() for f in futures]

    # Combine
    result = combine_chunk_results(results, len(pages))
    print(f"Pagination complete: {result.total_records} records in {result.chunks_processed} chunks")
    return result


if __name__ == "__main__":
    api_pagination_flow()
