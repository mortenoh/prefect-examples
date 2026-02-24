"""Register the DHIS2 connection deployment programmatically.

Usage:
    PREFECT_API_URL=http://localhost:4200/api uv run python deployments/dhis2_connection/deploy.py
"""
from flow import dhis2_connection_flow

if __name__ == "__main__":
    dhis2_connection_flow.deploy(
        name="dhis2-connection",
        work_pool_name="default",
        cron="*/15 * * * *",
    )
