"""076 -- Discriminated Unions.

Pydantic discriminated unions for type-safe polymorphic event dispatch.
Each event type has its own model and handler.

Airflow equivalent: Multi-API dashboard with heterogeneous sources (DAG 098).
Prefect approach:    Literal discriminator field, union type, dispatch @task.
"""

from typing import Annotated, Literal

from prefect import flow, task
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class ProcessingResult(BaseModel):
    """Result of processing a single event."""

    event_type: str
    event_id: str
    status: str
    details: str


class EmailEvent(BaseModel):
    """An email event."""

    event_type: Literal["email"] = "email"
    event_id: str
    sender: str
    recipient: str
    subject: str


class WebhookEvent(BaseModel):
    """A webhook event."""

    event_type: Literal["webhook"] = "webhook"
    event_id: str
    source_url: str
    payload_size: int


class ScheduleEvent(BaseModel):
    """A schedule-triggered event."""

    event_type: Literal["schedule"] = "schedule"
    event_id: str
    cron_expression: str
    job_name: str


Event = Annotated[
    EmailEvent | WebhookEvent | ScheduleEvent,
    Field(discriminator="event_type"),
]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def parse_events(raw: list[dict]) -> list[Event]:
    """Parse raw event dicts into typed Event objects.

    Args:
        raw: List of event dicts with event_type discriminator.

    Returns:
        List of typed Event objects.
    """
    from pydantic import TypeAdapter

    adapter = TypeAdapter(Event)
    events = [adapter.validate_python(r) for r in raw]
    print(f"Parsed {len(events)} events")
    return events


@task
def process_email_event(event: EmailEvent) -> ProcessingResult:
    """Process an email event.

    Args:
        event: The email event.

    Returns:
        ProcessingResult.
    """
    return ProcessingResult(
        event_type="email",
        event_id=event.event_id,
        status="processed",
        details=f"Email from {event.sender} to {event.recipient}: {event.subject}",
    )


@task
def process_webhook_event(event: WebhookEvent) -> ProcessingResult:
    """Process a webhook event.

    Args:
        event: The webhook event.

    Returns:
        ProcessingResult.
    """
    return ProcessingResult(
        event_type="webhook",
        event_id=event.event_id,
        status="processed",
        details=f"Webhook from {event.source_url} ({event.payload_size} bytes)",
    )


@task
def process_schedule_event(event: ScheduleEvent) -> ProcessingResult:
    """Process a schedule event.

    Args:
        event: The schedule event.

    Returns:
        ProcessingResult.
    """
    return ProcessingResult(
        event_type="schedule",
        event_id=event.event_id,
        status="processed",
        details=f"Scheduled job '{event.job_name}' ({event.cron_expression})",
    )


@task
def route_event(event: Event) -> ProcessingResult:
    """Route an event to its type-specific handler.

    Args:
        event: A typed Event.

    Returns:
        ProcessingResult from the appropriate handler.
    """
    if isinstance(event, EmailEvent):
        return process_email_event.fn(event)
    elif isinstance(event, WebhookEvent):
        return process_webhook_event.fn(event)
    elif isinstance(event, ScheduleEvent):
        return process_schedule_event.fn(event)
    return ProcessingResult(
        event_type="unknown",
        event_id="unknown",
        status="error",
        details="Unrecognised event type",
    )


@task
def summarize_processing(results: list[ProcessingResult]) -> dict:
    """Summarise processing results by event type.

    Args:
        results: List of processing results.

    Returns:
        Summary dict.
    """
    by_type: dict[str, int] = {}
    for r in results:
        by_type[r.event_type] = by_type.get(r.event_type, 0) + 1
    return {
        "total_events": len(results),
        "by_type": by_type,
        "all_processed": all(r.status == "processed" for r in results),
    }


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="076_discriminated_unions", log_prints=True)
def discriminated_unions_flow(raw_events: list[dict] | None = None) -> dict:
    """Process heterogeneous events using discriminated unions.

    Args:
        raw_events: Raw event dicts. Uses defaults if None.

    Returns:
        Processing summary dict.
    """
    if raw_events is None:
        raw_events = [
            {
                "event_type": "email",
                "event_id": "e1",
                "sender": "alice@example.com",
                "recipient": "bob@example.com",
                "subject": "Hello",
            },
            {
                "event_type": "webhook",
                "event_id": "w1",
                "source_url": "https://api.example.com/hook",
                "payload_size": 1024,
            },
            {"event_type": "schedule", "event_id": "s1", "cron_expression": "0 6 * * *", "job_name": "daily_report"},
            {
                "event_type": "email",
                "event_id": "e2",
                "sender": "charlie@example.com",
                "recipient": "diana@example.com",
                "subject": "Update",
            },
            {
                "event_type": "webhook",
                "event_id": "w2",
                "source_url": "https://api.other.com/notify",
                "payload_size": 512,
            },
        ]

    events = parse_events(raw_events)
    results = [route_event(event) for event in events]
    summary = summarize_processing(results)
    print(f"Processed {summary['total_events']} events: {summary['by_type']}")
    return summary


if __name__ == "__main__":
    discriminated_unions_flow()
