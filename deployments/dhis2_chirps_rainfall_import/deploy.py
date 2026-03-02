from dotenv import load_dotenv
from flow import dhis2_chirps_rainfall_import_flow

if __name__ == "__main__":
    load_dotenv()
    dhis2_chirps_rainfall_import_flow.deploy(
        name="dhis2-chirps-rainfall-import",
        work_pool_name="default",
    )
