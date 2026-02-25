"""Register S3 Parquet Export deployment programmatically.

Usage:
    PREFECT_API_URL=http://localhost:4200/api uv run python deployments/s3_parquet_export/deploy.py
"""

from dotenv import load_dotenv
from flow import s3_parquet_export_flow

if __name__ == "__main__":
    load_dotenv()
    s3_parquet_export_flow.deploy(
        name="s3-parquet-export",
        work_pool_name="default",
        cron="0 * * * *",
    )
