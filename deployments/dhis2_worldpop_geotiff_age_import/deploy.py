from dotenv import load_dotenv
from flow import dhis2_worldpop_geotiff_age_import_flow

if __name__ == "__main__":
    load_dotenv()
    dhis2_worldpop_geotiff_age_import_flow.deploy(
        name="dhis2-worldpop-geotiff-age-import",
        work_pool_name="default",
    )
