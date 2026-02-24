"""Events.

Emit custom Prefect events for observability and automation triggers.

Airflow equivalent: custom XCom + trigger rules.
Prefect approach:    emit_event() for signaling; automations for event-driven triggers.
"""

from prefect import flow, task
from prefect.events import emit_event


@task
def produce_data() -> str:
    """Produce a data string to be passed downstream."""
    data = "sample-payload-42"
    print(f"Produced data: {data}")
    return data


@task
def emit_completion_event(result: str) -> None:
    """Emit a custom Prefect event signaling that data was produced.

    Args:
        result: The data payload to include in the event.
    """
    emit_event(
        event="flow.data.produced",
        resource={"prefect.resource.id": "prefect_examples.014"},
        payload={"result": result},
    )
    print(f"Emitted event 'flow.data.produced' with result={result}")


@flow(name="basics_events", log_prints=True)
def events_flow() -> None:
    """Produce data and emit a completion event."""
    data = produce_data()
    emit_completion_event(data)


if __name__ == "__main__":
    events_flow()
