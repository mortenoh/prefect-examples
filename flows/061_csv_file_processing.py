"""061 -- CSV File Processing.

File-based ETL pipeline using the stdlib csv module. Demonstrates reading,
validating, transforming, and writing CSV files with an archive step.

Airflow equivalent: CSV landing zone pipeline (DAG 063).
Prefect approach:    @task functions handle each file operation; @flow orchestrates
                     the pipeline using tempfile for working directories.
"""

import csv
import shutil
import tempfile
from pathlib import Path

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class CsvRecord(BaseModel):
    """A single CSV row with validation metadata."""

    row_number: int
    data: dict
    valid: bool = True
    errors: list[str] = []


class CsvProcessingResult(BaseModel):
    """Summary of CSV processing."""

    total_rows: int
    valid_rows: int
    invalid_rows: int
    columns: list[str]
    output_path: str


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def generate_csv(directory: str, filename: str, rows: int = 10) -> Path:
    """Generate a sample CSV file with synthetic data.

    Args:
        directory: Directory to write the file in.
        filename: Name of the CSV file.
        rows: Number of data rows to generate.

    Returns:
        Path to the generated CSV file.
    """
    path = Path(directory) / filename
    fieldnames = ["id", "name", "email", "age", "score"]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(1, rows + 1):
            writer.writerow(
                {
                    "id": str(i),
                    "name": f"user_{i}",
                    "email": f"user_{i}@example.com",
                    "age": str(20 + (i % 40)),
                    "score": str(round(50.0 + (i * 3.7) % 50, 1)),
                }
            )
    print(f"Generated {rows} rows in {path}")
    return path


@task
def read_csv(path: Path) -> list[dict]:
    """Read a CSV file into a list of dicts.

    Args:
        path: Path to the CSV file.

    Returns:
        List of row dicts.
    """
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    print(f"Read {len(rows)} rows from {path.name}")
    return rows


@task
def validate_csv_row(row: dict, row_number: int, required_columns: list[str]) -> CsvRecord:
    """Validate a single CSV row.

    Args:
        row: The row dict to validate.
        row_number: 1-based row number.
        required_columns: Columns that must be non-empty.

    Returns:
        A CsvRecord with validation status.
    """
    errors: list[str] = []
    for col in required_columns:
        if col not in row or not row[col].strip():
            errors.append(f"Missing or empty required column: {col}")
    return CsvRecord(
        row_number=row_number,
        data=row,
        valid=len(errors) == 0,
        errors=errors,
    )


@task
def transform_csv_rows(records: list[CsvRecord]) -> list[dict]:
    """Transform valid CSV records.

    Args:
        records: List of validated CsvRecords.

    Returns:
        List of transformed row dicts (valid rows only, with uppercased names).
    """
    transformed = []
    for record in records:
        if not record.valid:
            continue
        row = dict(record.data)
        if "name" in row:
            row["name"] = row["name"].upper()
        if "score" in row:
            row["score"] = str(round(float(row["score"]) * 1.1, 1))
        transformed.append(row)
    print(f"Transformed {len(transformed)} valid rows")
    return transformed


@task
def write_csv(directory: str, filename: str, rows: list[dict]) -> Path:
    """Write rows to a CSV file.

    Args:
        directory: Output directory.
        filename: Output filename.
        rows: List of row dicts.

    Returns:
        Path to the written file.
    """
    if not rows:
        path = Path(directory) / filename
        path.write_text("")
        return path
    path = Path(directory) / filename
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {path}")
    return path


@task
def archive_file(source: Path, archive_dir: str) -> Path:
    """Move a file to the archive directory.

    Args:
        source: Path to the source file.
        archive_dir: Directory to archive into.

    Returns:
        Path to the archived file.
    """
    dest = Path(archive_dir) / source.name
    shutil.move(str(source), str(dest))
    print(f"Archived {source.name} to {archive_dir}")
    return dest


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="061_csv_file_processing", log_prints=True)
def csv_file_processing_flow(work_dir: str | None = None) -> CsvProcessingResult:
    """Process a CSV file: generate, read, validate, transform, write, archive.

    Args:
        work_dir: Working directory. Uses a temp dir if not provided.

    Returns:
        Processing result summary.
    """
    if work_dir is None:
        work_dir = tempfile.mkdtemp(prefix="csv_processing_")

    base = Path(work_dir)
    landing = base / "landing"
    output = base / "output"
    archive = base / "archive"
    for d in [landing, output, archive]:
        d.mkdir(parents=True, exist_ok=True)

    # Generate
    csv_path = generate_csv(str(landing), "input.csv", rows=10)

    # Read
    rows = read_csv(csv_path)

    # Validate
    required = ["id", "name", "email"]
    records = [validate_csv_row(row, i + 1, required) for i, row in enumerate(rows)]

    # Transform
    transformed = transform_csv_rows(records)

    # Write output
    output_path = write_csv(str(output), "processed.csv", transformed)

    # Archive original
    archive_file(csv_path, str(archive))

    columns = list(rows[0].keys()) if rows else []
    valid_count = sum(1 for r in records if r.valid)

    result = CsvProcessingResult(
        total_rows=len(rows),
        valid_rows=valid_count,
        invalid_rows=len(rows) - valid_count,
        columns=columns,
        output_path=str(output_path),
    )
    print(f"CSV processing complete: {result.valid_rows}/{result.total_rows} valid")
    return result


if __name__ == "__main__":
    csv_file_processing_flow()
