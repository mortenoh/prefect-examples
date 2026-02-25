"""Register DHIS2 GeoParquet export deployments programmatically.

Creates one deployment per DHIS2 instance.

Usage:
    PREFECT_API_URL=http://localhost:4200/api uv run python deployments/dhis2_geoparquet_export/deploy.py
"""

from flow import dhis2_geoparquet_export_flow

INSTANCES = [
    ("dhis2-geoparquet-export-dev", "dhis2-dev"),
    ("dhis2-geoparquet-export-v42", "dhis2-v42"),
    ("dhis2-geoparquet-export-v41", "dhis2-v41"),
    ("dhis2-geoparquet-export-v40", "dhis2-v40"),
]

if __name__ == "__main__":
    for name, instance in INSTANCES:
        dhis2_geoparquet_export_flow.deploy(
            name=name,
            work_pool_name="default",
            cron="0 * * * *",
            parameters={"instance": instance},
        )
