"""Remote Result Storage.

Persist task and flow results to S3-compatible storage (MinIO/RustFS)
using prefect-aws's S3Bucket as the result_storage backend.

Airflow equivalent: Custom XCom backend with S3 serialization.
Prefect approach:   result_storage=S3Bucket(...) on @task/@flow via
                    .with_options(), result_storage_key for addressable results.
"""

from __future__ import annotations

import logging
from typing import Any

from dotenv import load_dotenv
from prefect import flow, task
from prefect_aws import MinIOCredentials, S3Bucket
from prefect_aws.client_parameters import AwsClientParameters
from pydantic import SecretStr

logger = logging.getLogger(__name__)


def create_s3_storage() -> str | None:
    """Build, save, and return the slug for an S3Bucket block on MinIO.

    Blocks must be persisted server-side before they can be used as
    result_storage.  Returns None when S3 is not reachable so the flow
    can fall back to default local storage gracefully.

    Returns:
        Block slug string (e.g. "s3-bucket/minio-results") or None.
    """
    try:
        creds = MinIOCredentials(
            minio_root_user="admin",
            minio_root_password=SecretStr("admin"),
            aws_client_parameters=AwsClientParameters(
                endpoint_url="http://localhost:9000",
            ),
        )
        bucket = S3Bucket(
            bucket_name="prefect-results",
            credentials=creds,
        )
        # Verify S3 connectivity before registering the block --
        # .save() only stores config in the Prefect server, it does not
        # check whether the bucket is actually reachable.
        bucket.write_path("_prefect_health_check", b"ok")
        bucket.save("minio-results", overwrite=True)
        print("Registered S3Bucket block 's3-bucket/minio-results' for result storage")
        return "s3-bucket/minio-results"
    except Exception:
        logger.warning("S3 not available -- results will use default local storage")
        return None


# ---------------------------------------------------------------------------
# Tasks -- defined with persist_result=True; result_storage is applied
# at call time via .with_options() so the block can be saved first.
# ---------------------------------------------------------------------------


@task(persist_result=True)
def generate_report_data(region: str) -> dict[str, Any]:
    """Generate sample report data for a given region.

    Demonstrates basic remote result persistence -- the returned dict
    is serialised and stored in the configured S3 bucket.

    Args:
        region: Geographic region identifier.

    Returns:
        A dict of sample report metrics.
    """
    data = {
        "region": region,
        "total_sales": 150_000,
        "units_sold": 3_200,
        "avg_price": round(150_000 / 3_200, 2),
        "top_product": "Widget Pro",
    }
    print(f"Generated report data for {region}: {data}")
    return data


@task(persist_result=True, result_storage_key="enriched-{parameters[region]}")
def enrich_data(report: dict[str, Any], region: str) -> dict[str, Any]:
    """Enrich report data with derived metrics.

    Uses a custom result_storage_key so the result is stored at a
    predictable, addressable path (e.g. enriched-us-east).

    Args:
        report: Raw report data dict.
        region: Region identifier (used in the storage key).

    Returns:
        Enriched report dict with additional fields.
    """
    enriched = {
        **report,
        "margin_pct": 32.5,
        "growth_yoy": 12.1,
        "tier": "gold" if report["total_sales"] > 100_000 else "silver",
    }
    print(f"Enriched data for {region}: tier={enriched['tier']}")
    return enriched


@task(persist_result=True)
def summarize(reports: list[dict[str, Any]]) -> str:
    """Summarise multiple enriched reports into a single string.

    This task uses persist_result=True but does NOT set result_storage
    explicitly, so it inherits the flow-level result_storage setting.

    Args:
        reports: List of enriched report dicts.

    Returns:
        A formatted summary string.
    """
    total_sales = sum(r["total_sales"] for r in reports)
    total_units = sum(r["units_sold"] for r in reports)
    regions = [r["region"] for r in reports]
    summary = (
        f"Summary across {len(reports)} regions ({', '.join(regions)}): "
        f"total_sales={total_sales:,}, total_units={total_units:,}"
    )
    print(summary)
    return summary


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="core_remote_result_storage", log_prints=True, persist_result=True)
def remote_result_storage_flow(
    regions: list[str] | None = None,
) -> str:
    """Demonstrate remote result storage with S3-compatible backends.

    Creates an S3Bucket block at runtime, then applies it to tasks via
    .with_options(result_storage=...).  Tasks that do not override
    result_storage (e.g. summarize) inherit the flow-level default.

    Args:
        regions: List of region identifiers to process.

    Returns:
        A summary string across all regions.
    """
    if regions is None:
        regions = ["us-east", "us-west", "eu-central"]

    storage = create_s3_storage()

    enriched_reports = []
    for region in regions:
        if storage:
            report = generate_report_data.with_options(
                result_storage=storage,
            )(region)
            enriched = enrich_data.with_options(
                result_storage=storage,
            )(report, region)
        else:
            report = generate_report_data(region)
            enriched = enrich_data(report, region)
        enriched_reports.append(enriched)

    return summarize(enriched_reports)


if __name__ == "__main__":
    load_dotenv()
    remote_result_storage_flow()
