"""Register the DHIS2 World Bank health import deployment programmatically.

Usage:
    PREFECT_API_URL=http://localhost:4200/api uv run python deployments/dhis2_worldbank_health_import/deploy.py
"""

from dotenv import load_dotenv
from flow import dhis2_worldbank_health_import_flow

if __name__ == "__main__":
    load_dotenv()
    dhis2_worldbank_health_import_flow.deploy(
        name="dhis2-worldbank-health-import",
        work_pool_name="default",
    )
