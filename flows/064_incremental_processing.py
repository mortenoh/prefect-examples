"""064 -- Incremental Processing.

Manifest-based incremental file processing. A JSON manifest tracks which files
have been processed; subsequent runs only process new files.

Airflow equivalent: Manifest-based incremental file processing (DAG 067).
Prefect approach:    JSON manifest file, scan-and-diff pattern, .map() for
                     parallel file processing.
"""

import csv
import datetime
import json
import tempfile
from pathlib import Path

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class ProcessingManifest(BaseModel):
    """Tracks which files have been processed."""

    last_updated: str = ""
    processed_files: dict[str, str] = {}
    total_processed: int = 0


class IncrementalResult(BaseModel):
    """Result of an incremental processing run."""

    new_files: int
    skipped_files: int
    records_processed: int


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def load_manifest(path: Path) -> ProcessingManifest:
    """Load the processing manifest from disk.

    Args:
        path: Path to the manifest JSON file.

    Returns:
        The manifest (empty if file does not exist).
    """
    if path.exists():
        data = json.loads(path.read_text())
        manifest = ProcessingManifest(**data)
        print(f"Loaded manifest: {manifest.total_processed} files previously processed")
        return manifest
    print("No manifest found, starting fresh")
    return ProcessingManifest()


@task
def scan_directory(directory: str, pattern: str = "*.csv") -> list[Path]:
    """Scan a directory for files matching a pattern.

    Args:
        directory: Directory to scan.
        pattern: Glob pattern.

    Returns:
        Sorted list of matching paths.
    """
    files = sorted(Path(directory).glob(pattern))
    print(f"Found {len(files)} files matching {pattern}")
    return files


@task
def identify_new_files(all_files: list[Path], manifest: ProcessingManifest) -> list[Path]:
    """Identify files not yet in the manifest.

    Args:
        all_files: All discovered files.
        manifest: The current processing manifest.

    Returns:
        List of new (unprocessed) file paths.
    """
    new = [f for f in all_files if f.name not in manifest.processed_files]
    print(f"Identified {len(new)} new files out of {len(all_files)} total")
    return new


@task
def process_file(path: Path) -> dict:
    """Process a single file and return record count.

    Args:
        path: Path to the file.

    Returns:
        Dict with filename and record count.
    """
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    print(f"Processed {path.name}: {len(rows)} records")
    return {"filename": path.name, "records": len(rows)}


@task
def update_manifest(manifest: ProcessingManifest, results: list[dict], path: Path) -> ProcessingManifest:
    """Update and save the manifest with newly processed files.

    Args:
        manifest: Current manifest.
        results: List of processing results.
        path: Path to save the manifest.

    Returns:
        Updated manifest.
    """
    now = datetime.datetime.now(datetime.UTC).isoformat()
    for r in results:
        manifest.processed_files[r["filename"]] = now
    manifest.total_processed = len(manifest.processed_files)
    manifest.last_updated = now
    path.write_text(json.dumps(manifest.model_dump(), indent=2))
    print(f"Updated manifest: {manifest.total_processed} total files")
    return manifest


@task
def incremental_report(new_files: int, skipped: int, records: int) -> str:
    """Generate a summary report.

    Args:
        new_files: Number of new files processed.
        skipped: Number of files skipped.
        records: Total records processed.

    Returns:
        Summary string.
    """
    return f"Processed {new_files} new files ({records} records), skipped {skipped} already-processed files"


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="064_incremental_processing", log_prints=True)
def incremental_processing_flow(work_dir: str | None = None) -> IncrementalResult:
    """Run incremental file processing with manifest tracking.

    Args:
        work_dir: Working directory. Uses temp dir if not provided.

    Returns:
        Incremental processing result.
    """
    if work_dir is None:
        work_dir = tempfile.mkdtemp(prefix="incremental_")

    base = Path(work_dir)
    data_dir = base / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = base / "manifest.json"

    # Generate sample data if none exists
    if not list(data_dir.glob("*.csv")):
        _generate_sample_data(data_dir)

    # Load manifest
    manifest = load_manifest(manifest_path)

    # Scan and identify new files
    all_files = scan_directory(str(data_dir), "*.csv")
    new_files = identify_new_files(all_files, manifest)

    # Process new files
    if new_files:
        futures = process_file.map(new_files)
        results = [f.result() for f in futures]
        total_records = sum(r["records"] for r in results)
    else:
        results = []
        total_records = 0

    # Update manifest
    update_manifest(manifest, results, manifest_path)

    skipped = len(all_files) - len(new_files)
    report = incremental_report(len(new_files), skipped, total_records)
    print(report)

    return IncrementalResult(
        new_files=len(new_files),
        skipped_files=skipped,
        records_processed=total_records,
    )


def _generate_sample_data(directory: Path) -> None:
    """Generate sample CSV files."""
    for i in range(1, 4):
        path = directory / f"batch_{i:03d}.csv"
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "value"])
            writer.writeheader()
            for j in range(5):
                writer.writerow({"id": str(i * 100 + j), "value": str(j * 10.5)})


if __name__ == "__main__":
    incremental_processing_flow()
