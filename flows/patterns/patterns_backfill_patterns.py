"""Backfill Patterns.

Parameterized pipelines for date-range processing and gap detection.

Airflow equivalent: Backfill awareness, parameterized pipelines (DAGs 080, 108).
Prefect approach:    Flow parameters for date ranges, gap detection logic.
"""

import datetime
from typing import Any

from dotenv import load_dotenv
from prefect import flow, task

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def process_date(date_str: str) -> dict[str, Any]:
    """Process data for a single date.

    Args:
        date_str: The date to process in YYYY-MM-DD format.

    Returns:
        A dict with the processing result.
    """
    result = {"date": date_str, "records": len(date_str) * 10, "status": "processed"}
    print(f"Processed {date_str}: {result['records']} records")
    return result


@task
def detect_gaps(processed_dates: list[str], start_date: str, end_date: str) -> list[str]:
    """Detect unprocessed dates in a date range.

    Args:
        processed_dates: Dates that have already been processed.
        start_date: Start of the expected range (YYYY-MM-DD).
        end_date: End of the expected range (YYYY-MM-DD).

    Returns:
        A list of dates that are missing from the processed set.
    """
    start = datetime.date.fromisoformat(start_date)
    end = datetime.date.fromisoformat(end_date)
    all_dates = set()
    current = start
    while current <= end:
        all_dates.add(current.isoformat())
        current += datetime.timedelta(days=1)

    gaps = sorted(all_dates - set(processed_dates))
    print(f"Found {len(gaps)} gaps in date range {start_date} to {end_date}")
    return gaps


@task
def backfill_report(original: list[dict[str, Any]], backfilled: list[dict[str, Any]]) -> str:
    """Generate a report on backfill results.

    Args:
        original: Results from the original processing run.
        backfilled: Results from the backfill run.

    Returns:
        A summary report string.
    """
    report = (
        f"Backfill report: {len(original)} original, {len(backfilled)} backfilled, "
        f"{len(original) + len(backfilled)} total"
    )
    print(report)
    return report


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="patterns_backfill_patterns", log_prints=True)
def backfill_patterns_flow(
    start_date: str = "2024-01-01",
    end_date: str = "2024-01-05",
) -> None:
    """Process a date range with gap detection and backfill.

    Args:
        start_date: Start of the date range.
        end_date: End of the date range.
    """
    # Initial processing (simulate only processing some dates)
    initial_dates = ["2024-01-01", "2024-01-03", "2024-01-05"]
    initial_futures = process_date.map(initial_dates)
    initial_results = [f.result() for f in initial_futures]

    # Detect gaps
    gaps = detect_gaps(initial_dates, start_date, end_date)

    # Backfill missing dates
    if gaps:
        backfill_futures = process_date.map(gaps)
        backfill_results = [f.result() for f in backfill_futures]
    else:
        backfill_results = []

    backfill_report(initial_results, backfill_results)


if __name__ == "__main__":
    load_dotenv()
    backfill_patterns_flow()
