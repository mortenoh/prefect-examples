"""Register the DHIS2 org unit deployment programmatically.

Usage:
    PREFECT_API_URL=http://localhost:4200/api uv run python deployments/dhis2_ou/deploy.py
"""

from flow import dhis2_ou_flow

if __name__ == "__main__":
    dhis2_ou_flow.deploy(
        name="dhis2-ou",
        work_pool_name="default",
        cron="*/15 * * * *",
    )
