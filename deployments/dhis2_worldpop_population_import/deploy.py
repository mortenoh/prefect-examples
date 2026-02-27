"""Register the DHIS2 WorldPop population import deployment programmatically.

Usage:
    PREFECT_API_URL=http://localhost:4200/api uv run python deployments/dhis2_worldpop_population_import/deploy.py
"""

from dotenv import load_dotenv
from flow import dhis2_worldpop_population_import_flow

if __name__ == "__main__":
    load_dotenv()
    dhis2_worldpop_population_import_flow.deploy(
        name="dhis2-worldpop-population-import",
        work_pool_name="default",
    )
