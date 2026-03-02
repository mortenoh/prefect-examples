FROM prefecthq/prefect:3-python3.13

COPY packages/prefect-dhis2 /tmp/prefect-dhis2
COPY packages/prefect-worldpop /tmp/prefect-worldpop
RUN pip install --no-cache-dir /tmp/prefect-dhis2 /tmp/prefect-worldpop python-dotenv pydantic \
    && rm -rf /tmp/prefect-dhis2 /tmp/prefect-worldpop

COPY deployments/dhis2_worldpop_geotiff_import_docker/flow.py /opt/prefect/flow.py

WORKDIR /opt/prefect
CMD ["python", "flow.py"]
