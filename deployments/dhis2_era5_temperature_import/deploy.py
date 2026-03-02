from dotenv import load_dotenv
from flow import dhis2_era5_temperature_import_flow

if __name__ == "__main__":
    load_dotenv()
    dhis2_era5_temperature_import_flow.deploy(
        name="dhis2-era5-temperature-import",
        work_pool_name="default",
    )
