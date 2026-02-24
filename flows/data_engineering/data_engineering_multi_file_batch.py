"""Multi-File Batch Processing.

Discovers, reads, and unifies records from mixed CSV and JSON files. Applies
column harmonisation via rename mapping and hash-based deduplication.

Airflow equivalent: Mixed CSV+JSON batch processing (DAG 065).
Prefect approach:    File-type dispatch in a @task, column mapping, hashlib dedup.
"""

import csv
import hashlib
import json
import tempfile
from pathlib import Path

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class UnifiedRecord(BaseModel):
    """A unified record from any source file."""

    source_file: str
    format: str
    id: str
    name: str
    value: float
    record_hash: str


class BatchResult(BaseModel):
    """Summary of batch processing."""

    total_records: int
    unique_records: int
    duplicate_records: int
    format_counts: dict[str, int]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def discover_files(directory: str, extensions: list[str]) -> list[Path]:
    """Discover files matching given extensions.

    Args:
        directory: Directory to scan.
        extensions: File extensions to match (e.g., [".csv", ".json"]).

    Returns:
        Sorted list of matching file paths.
    """
    base = Path(directory)
    files = []
    for ext in extensions:
        files.extend(base.glob(f"*{ext}"))
    files.sort()
    print(f"Discovered {len(files)} files in {directory}")
    return files


@task
def read_file(path: Path) -> list[dict]:
    """Read a file based on its extension (CSV or JSON).

    Args:
        path: File path.

    Returns:
        List of record dicts.
    """
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            return list(reader)
    elif suffix == ".json":
        data = json.loads(path.read_text())
        if isinstance(data, list):
            return data
        return [data]
    else:
        raise ValueError(f"Unsupported file format: {suffix}")


@task
def harmonize_columns(records: list[dict], column_map: dict[str, str]) -> list[dict]:
    """Rename columns according to a mapping.

    Args:
        records: List of record dicts.
        column_map: Mapping of old column name to new column name.

    Returns:
        List of records with harmonised column names.
    """
    harmonized = []
    for record in records:
        new_record = {}
        for key, value in record.items():
            new_key = column_map.get(key, key)
            new_record[new_key] = value
        harmonized.append(new_record)
    return harmonized


@task
def compute_record_hash(record: dict, key_fields: list[str]) -> str:
    """Compute a hash for a record based on key fields.

    Args:
        record: The record dict.
        key_fields: Fields to include in the hash.

    Returns:
        Hex digest string.
    """
    hash_input = "|".join(str(record.get(f, "")) for f in sorted(key_fields))
    return hashlib.sha256(hash_input.encode()).hexdigest()[:16]


@task
def deduplicate(
    records: list[dict], source_files: list[str], formats: list[str], key_fields: list[str]
) -> list[UnifiedRecord]:
    """Deduplicate records based on hash of key fields.

    Args:
        records: All harmonised records.
        source_files: Source file for each record.
        formats: Format (csv/json) for each record.
        key_fields: Fields to use for deduplication.

    Returns:
        Deduplicated list of UnifiedRecords.
    """
    seen: set[str] = set()
    unique: list[UnifiedRecord] = []
    for record, source, fmt in zip(records, source_files, formats, strict=True):
        h = compute_record_hash.fn(record, key_fields)
        if h not in seen:
            seen.add(h)
            unique.append(
                UnifiedRecord(
                    source_file=source,
                    format=fmt,
                    id=str(record.get("id", "")),
                    name=str(record.get("name", "")),
                    value=float(record.get("value", 0)),
                    record_hash=h,
                )
            )
    return unique


@task
def summarize_batch(total: int, deduped: list[UnifiedRecord], format_counts: dict[str, int]) -> BatchResult:
    """Summarise batch processing results.

    Args:
        total: Total records before dedup.
        deduped: Deduplicated records.
        format_counts: Count of records per format.

    Returns:
        BatchResult summary.
    """
    return BatchResult(
        total_records=total,
        unique_records=len(deduped),
        duplicate_records=total - len(deduped),
        format_counts=format_counts,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="data_engineering_multi_file_batch", log_prints=True)
def multi_file_batch_flow(work_dir: str | None = None) -> BatchResult:
    """Process multiple CSV and JSON files into a unified, deduplicated dataset.

    Args:
        work_dir: Working directory. Uses temp dir if not provided.

    Returns:
        Batch result summary.
    """
    if work_dir is None:
        work_dir = tempfile.mkdtemp(prefix="multi_file_batch_")

    base = Path(work_dir)
    base.mkdir(parents=True, exist_ok=True)

    # Generate sample files
    _generate_sample_files(base)

    # Discover
    files = discover_files(str(base), [".csv", ".json"])

    # Read and collect
    all_records: list[dict] = []
    source_files: list[str] = []
    formats: list[str] = []
    format_counts: dict[str, int] = {}

    column_map = {"user_id": "id", "user_name": "name", "amount": "value"}

    for file_path in files:
        raw = read_file(file_path)
        harmonized = harmonize_columns(raw, column_map)
        fmt = file_path.suffix.lstrip(".")
        format_counts[fmt] = format_counts.get(fmt, 0) + len(harmonized)
        for record in harmonized:
            all_records.append(record)
            source_files.append(file_path.name)
            formats.append(fmt)

    # Deduplicate
    deduped = deduplicate(all_records, source_files, formats, ["id", "name"])

    # Summarise
    result = summarize_batch(len(all_records), deduped, format_counts)
    print(f"Batch complete: {result.unique_records}/{result.total_records} unique")
    return result


def _generate_sample_files(directory: Path) -> None:
    """Generate sample CSV and JSON files for testing."""
    # CSV file
    csv_path = directory / "data_a.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "value"])
        writer.writeheader()
        writer.writerow({"id": "1", "name": "Alice", "value": "100"})
        writer.writerow({"id": "2", "name": "Bob", "value": "200"})
        writer.writerow({"id": "3", "name": "Charlie", "value": "300"})

    # JSON file with different column names (to test harmonisation)
    json_path = directory / "data_b.json"
    json_path.write_text(
        json.dumps(
            [
                {"user_id": "2", "user_name": "Bob", "amount": "200"},
                {"user_id": "4", "user_name": "Diana", "amount": "400"},
            ]
        )
    )


if __name__ == "__main__":
    multi_file_batch_flow()
