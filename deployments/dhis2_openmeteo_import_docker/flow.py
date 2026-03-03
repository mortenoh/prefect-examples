"""Open-Meteo deployment -- serves all three Open-Meteo flows.

Serves the historical weather, weather forecast, and air quality
import flows from a single container.
"""

from dhis2_openmeteo_air_quality_import import dhis2_openmeteo_air_quality_import_flow
from dhis2_openmeteo_forecast_import import dhis2_openmeteo_forecast_import_flow
from dhis2_openmeteo_historical_import import dhis2_openmeteo_historical_import_flow
from dotenv import load_dotenv
from prefect import serve

if __name__ == "__main__":
    load_dotenv()
    serve(
        dhis2_openmeteo_historical_import_flow.to_deployment(
            name="dhis2-openmeteo-historical-import-docker",
        ),
        dhis2_openmeteo_forecast_import_flow.to_deployment(
            name="dhis2-openmeteo-forecast-import-docker",
        ),
        dhis2_openmeteo_air_quality_import_flow.to_deployment(
            name="dhis2-openmeteo-air-quality-import-docker",
        ),
    )
