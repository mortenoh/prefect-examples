"""DHIS2 Connection -- deployment-ready flow.

Verifies DHIS2 connectivity, fetches org unit count, and reports status.

Three ways to register this deployment:

1. CLI::

    cd deployments/dhis2_connection
    prefect deploy --all

2. Declarative (prefect.yaml in this directory)::

    See prefect.yaml

3. Python::

    python deployments/dhis2_connection/deploy.py
"""

import os

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.blocks.notifications import SlackWebhook
from prefect.runtime import deployment
from pydantic import BaseModel, SecretStr

from prefect_examples.dhis2 import Dhis2Client, Dhis2Credentials, get_dhis2_credentials


class ConnectionReport(BaseModel):
    deployment_name: str
    host: str
    username: str
    server_version: str
    org_unit_count: int


@task
def verify_connection(client: Dhis2Client) -> dict:
    data = client.get_server_info()
    print(f"Connected to DHIS2 v{data.get('version', 'unknown')}")
    return data


@task
def fetch_org_unit_count(client: Dhis2Client) -> int:
    records = client.fetch_metadata("organisationUnits", fields="id")
    print(f"Organisation units: {len(records)}")
    return len(records)


@task
def build_report(
    creds: Dhis2Credentials,
    server_info: dict,
    org_unit_count: int,
) -> ConnectionReport:
    return ConnectionReport(
        deployment_name=deployment.name or "local",
        host=creds.base_url,
        username=creds.username,
        server_version=server_info.get("version", "unknown"),
        org_unit_count=org_unit_count,
    )


@flow(name="dhis2_connection", log_prints=True)
def dhis2_connection_flow() -> ConnectionReport:
    """Verify DHIS2 connectivity and report status."""
    creds = get_dhis2_credentials()
    client = creds.get_client()
    server_info = verify_connection(client)
    count = fetch_org_unit_count(client)
    report = build_report(creds, server_info, count)
    markdown = (
        f"# DHIS2 Connection Report\n\n"
        f"| Field | Value |\n"
        f"|-------|-------|\n"
        f"| Deployment | {report.deployment_name} |\n"
        f"| Host | {report.host} |\n"
        f"| User | {report.username} |\n"
        f"| Server version | {report.server_version} |\n"
        f"| Organisation units | {report.org_unit_count} |\n"
    )
    create_markdown_artifact(
        key="dhis2-connection-report",
        markdown=markdown,
        description="DHIS2 connection verification report",
    )
    print(
        f"[{report.deployment_name}] {report.username}@{report.host} "
        f"-- v{report.server_version}, {report.org_unit_count} org units"
    )
    slack_url = os.environ.get("SLACK_WEBHOOK_URL")
    if slack_url:
        slack = SlackWebhook(url=SecretStr(slack_url))
        slack.notify(
            body=(
                f"*{report.deployment_name}*: {report.username}@{report.host}\n"
                f"Server v{report.server_version} -- {report.org_unit_count} org units"
            ),
            subject="DHIS2 Connection Report",
        )
    return report


if __name__ == "__main__":
    dhis2_connection_flow()
