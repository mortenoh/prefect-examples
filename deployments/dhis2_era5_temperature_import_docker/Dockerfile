FROM prefecthq/prefect:3-python3.13

COPY packages/prefect-dhis2 /tmp/prefect-dhis2
COPY packages/prefect-climate /tmp/prefect-climate
RUN pip install --no-cache-dir /tmp/prefect-dhis2 /tmp/prefect-climate python-dotenv pydantic \
    && rm -rf /tmp/prefect-dhis2 /tmp/prefect-climate

COPY deployments/dhis2_era5_temperature_import_docker/flow.py /opt/prefect/flow.py

WORKDIR /opt/prefect
CMD ["python", "flow.py"]
