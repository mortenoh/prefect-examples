FROM prefecthq/prefect:3-python3.13

RUN apt-get update && apt-get install -y --no-install-recommends libgfortran5 \
    && rm -rf /var/lib/apt/lists/*

COPY packages/prefect-dhis2 /tmp/prefect-dhis2
COPY packages/prefect-climate /tmp/prefect-climate
RUN pip install --no-cache-dir /tmp/prefect-dhis2 /tmp/prefect-climate python-dotenv pydantic \
    && rm -rf /tmp/prefect-dhis2 /tmp/prefect-climate

COPY flows/dhis2/dhis2_openmeteo_historical_import.py /opt/prefect/dhis2_openmeteo_historical_import.py
COPY flows/dhis2/dhis2_openmeteo_forecast_import.py /opt/prefect/dhis2_openmeteo_forecast_import.py
COPY flows/dhis2/dhis2_openmeteo_air_quality_import.py /opt/prefect/dhis2_openmeteo_air_quality_import.py
COPY deployments/dhis2_openmeteo_import_docker/flow.py /opt/prefect/flow.py

WORKDIR /opt/prefect
CMD ["python", "flow.py"]
