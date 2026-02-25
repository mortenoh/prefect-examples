"""Register DHIS2 block connection deployments programmatically.

Creates one deployment per DHIS2 instance.

Usage:
    PREFECT_API_URL=http://localhost:4200/api uv run python deployments/dhis2_block_connection/deploy.py
"""

from flow import dhis2_block_connection_flow

INSTANCES = [
    ("dhis2-block-connection-dev", "dhis2-dev"),
    ("dhis2-block-connection-v42", "dhis2-stable-2-42-4"),
    ("dhis2-block-connection-v41", "dhis2-stable-2-41-7"),
    ("dhis2-block-connection-v40", "dhis2-stable-2-40-11"),
]

if __name__ == "__main__":
    for name, instance in INSTANCES:
        dhis2_block_connection_flow.deploy(
            name=name,
            work_pool_name="default",
            cron="*/15 * * * *",
            parameters={"instance": instance},
        )
