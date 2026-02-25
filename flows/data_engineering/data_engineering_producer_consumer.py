"""Producer-Consumer.

Cross-flow communication via file-based data contracts. A producer flow
writes data packages; a consumer flow discovers and processes them.

Airflow equivalent: Asset + XCom producer/consumer (DAG 112).
Prefect approach:    Separate producer and consumer flows, file-based
                     data packages with metadata.
"""

import datetime
import json
import tempfile
from pathlib import Path

from dotenv import load_dotenv
from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class DataPackage(BaseModel):
    """A data package produced by the producer."""

    producer_id: str
    produced_at: str
    record_count: int
    data_path: str
    metadata_path: str


class ConsumerResult(BaseModel):
    """Result of consuming a data package."""

    consumer_id: str
    package_producer: str
    records_processed: int
    latency_seconds: float


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def produce_data(output_dir: str, producer_id: str, records: int = 10) -> DataPackage:
    """Produce a data package and write it to disk.

    Args:
        output_dir: Directory to write the package.
        producer_id: Identifier for this producer.
        records: Number of records to produce.

    Returns:
        DataPackage descriptor.
    """
    base = Path(output_dir)
    base.mkdir(parents=True, exist_ok=True)

    now = datetime.datetime.now(datetime.UTC)
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    data_file = base / f"{producer_id}_{timestamp}.json"
    meta_file = base / f"{producer_id}_{timestamp}.meta.json"

    data = [{"id": i, "value": i * 7.5, "source": producer_id} for i in range(1, records + 1)]
    data_file.write_text(json.dumps(data))

    package = DataPackage(
        producer_id=producer_id,
        produced_at=now.isoformat(),
        record_count=records,
        data_path=str(data_file),
        metadata_path=str(meta_file),
    )
    meta_file.write_text(json.dumps(package.model_dump()))
    print(f"Producer '{producer_id}' wrote {records} records")
    return package


@task
def discover_packages(data_dir: str) -> list[DataPackage]:
    """Discover data packages by finding metadata files.

    Args:
        data_dir: Directory to scan.

    Returns:
        List of DataPackage objects.
    """
    base = Path(data_dir)
    packages = []
    for meta_file in sorted(base.glob("*.meta.json")):
        data = json.loads(meta_file.read_text())
        packages.append(DataPackage(**data))
    print(f"Discovered {len(packages)} packages")
    return packages


@task
def consume_package(package: DataPackage, consumer_id: str) -> ConsumerResult:
    """Consume a single data package.

    Args:
        package: The data package to consume.
        consumer_id: Identifier for this consumer.

    Returns:
        ConsumerResult.
    """
    data = json.loads(Path(package.data_path).read_text())
    produced_time = datetime.datetime.fromisoformat(package.produced_at)
    latency = (datetime.datetime.now(datetime.UTC) - produced_time).total_seconds()
    print(f"Consumer '{consumer_id}' processed {len(data)} records from '{package.producer_id}'")
    return ConsumerResult(
        consumer_id=consumer_id,
        package_producer=package.producer_id,
        records_processed=len(data),
        latency_seconds=round(latency, 2),
    )


@task
def consumption_report(results: list[ConsumerResult]) -> str:
    """Generate a consumption summary report.

    Args:
        results: List of consumer results.

    Returns:
        Summary string.
    """
    total = sum(r.records_processed for r in results)
    return f"Consumed {len(results)} packages, {total} total records"


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------


@flow(name="data_engineering_producer", log_prints=True)
def producer_flow(output_dir: str, producer_id: str = "producer_1", records: int = 10) -> DataPackage:
    """Produce data packages.

    Args:
        output_dir: Output directory.
        producer_id: Producer identifier.
        records: Records per package.

    Returns:
        DataPackage.
    """
    return produce_data(output_dir, producer_id, records)


@flow(name="data_engineering_consumer", log_prints=True)
def consumer_flow(data_dir: str, consumer_id: str = "consumer_1") -> list[ConsumerResult]:
    """Consume all available data packages.

    Args:
        data_dir: Directory to scan for packages.
        consumer_id: Consumer identifier.

    Returns:
        List of ConsumerResult.
    """
    packages = discover_packages(data_dir)
    results = [consume_package(pkg, consumer_id) for pkg in packages]
    report = consumption_report(results)
    print(report)
    return results


@flow(name="data_engineering_producer_consumer", log_prints=True)
def producer_consumer_flow(work_dir: str | None = None) -> list[ConsumerResult]:
    """Orchestrate production and consumption via tasks.

    Args:
        work_dir: Working directory. Uses temp dir if not provided.

    Returns:
        Consumer results.
    """
    if work_dir is None:
        work_dir = tempfile.mkdtemp(prefix="producer_consumer_")

    data_dir = str(Path(work_dir) / "packages")

    # Produce
    produce_data(data_dir, "alpha", records=8)
    produce_data(data_dir, "beta", records=12)

    # Consume
    packages = discover_packages(data_dir)
    results = [consume_package(pkg, "main_consumer") for pkg in packages]
    report = consumption_report(results)
    print(report)
    return results


if __name__ == "__main__":
    load_dotenv()
    producer_consumer_flow()
