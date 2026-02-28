FROM prefecthq/prefect:3-python3.13

COPY packages/prefect-dhis2 /tmp/prefect-dhis2
RUN pip install --no-cache-dir /tmp/prefect-dhis2 python-dotenv pydantic \
    && rm -rf /tmp/prefect-dhis2

COPY deployments/dhis2_worldpop_population_import_docker/flow.py /opt/prefect/flow.py

WORKDIR /opt/prefect
CMD ["python", "flow.py"]
