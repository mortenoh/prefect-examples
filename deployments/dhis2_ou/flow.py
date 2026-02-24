"""DHIS2 Organisation Units -- deployment-ready flow.

Fetches org units from DHIS2 and produces a markdown artifact listing
each unit's ID and display name.

Three ways to register this deployment:

1. CLI::

    cd deployments/dhis2_ou
    prefect deploy --all

2. Declarative (prefect.yaml in this directory)::

    See prefect.yaml

3. Python::

    python deployments/dhis2_ou/deploy.py
"""
from __future__ import annotations

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.runtime import deployment
from pydantic import BaseModel

from prefect_examples.dhis2 import Dhis2Client, get_dhis2_credentials


class OrgUnitReport(BaseModel):
    deployment_name: str
    org_unit_count: int
    markdown: str


@task
def fetch_org_units(client: Dhis2Client) -> list[dict]:
    """Fetch organisation units with id and displayName only."""
    records = client.fetch_metadata("organisationUnits", fields="id,displayName")
    print(f"Fetched {len(records)} organisation units")
    return records


@task
def build_report(org_units: list[dict]) -> OrgUnitReport:
    """Build a markdown table of organisation units and create an artifact."""
    dep_name = deployment.name or "local"

    lines = [
        f"# DHIS2 Organisation Units ({dep_name})",
        "",
        f"**Total:** {len(org_units)} units",
        "",
        "| ID | Name |",
        "|----|------|",
    ]
    for ou in org_units:
        lines.append(f"| {ou['id']} | {ou['displayName']} |")

    markdown = "\n".join(lines)

    create_markdown_artifact(
        key="dhis2-org-units",
        markdown=markdown,
        description="DHIS2 organisation unit listing",
    )

    return OrgUnitReport(
        deployment_name=dep_name,
        org_unit_count=len(org_units),
        markdown=markdown,
    )


@flow(name="dhis2_ou", log_prints=True)
def dhis2_ou_flow() -> OrgUnitReport:
    """Fetch DHIS2 organisation units and produce a markdown report."""
    client = get_dhis2_credentials().get_client()
    org_units = fetch_org_units(client)
    report = build_report(org_units)
    print(f"[{report.deployment_name}] {report.org_unit_count} org units")
    return report


if __name__ == "__main__":
    dhis2_ou_flow()
