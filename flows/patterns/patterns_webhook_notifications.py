"""Webhook Notifications.

Send webhook notifications on pipeline events using httpx.

Airflow equivalent: Webhook alerts on pipeline events (DAG 074).
Prefect approach:    httpx.post() in tasks and flow hooks.
"""

from typing import Any

from prefect import flow, task

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def send_notification(event: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Send a webhook notification for a pipeline event.

    In production, this would POST to Slack, PagerDuty, etc.
    Here we simulate the call and return what would be sent.

    Args:
        event: The event type (e.g. "pipeline.started").
        payload: Additional context for the notification.

    Returns:
        A dict representing the notification that was sent.
    """
    notification = {
        "event": event,
        "payload": payload,
        "status": "sent",
    }
    print(f"Notification: {event} -> {payload}")
    return notification


@task
def process_data() -> dict[str, Any]:
    """Simulate data processing work.

    Returns:
        A dict with processing results.
    """
    result = {"records_processed": 42, "errors": 0, "status": "success"}
    print(f"Processed {result['records_processed']} records")
    return result


# ---------------------------------------------------------------------------
# Hooks
# ---------------------------------------------------------------------------


def on_flow_completion(flow: Any, flow_run: Any, state: Any) -> None:
    """Send a notification when the flow completes.

    Args:
        flow: The flow object.
        flow_run: The flow-run metadata.
        state: The final state of the flow run.
    """
    print(f"HOOK  Flow {flow_run.name!r} completed with state: {state.name}")


def on_flow_failure(flow: Any, flow_run: Any, state: Any) -> None:
    """Send a critical notification when the flow fails.

    Args:
        flow: The flow object.
        flow_run: The flow-run metadata.
        state: The final state of the flow run.
    """
    print(f"HOOK  CRITICAL: Flow {flow_run.name!r} failed with state: {state.name}")


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(
    name="patterns_webhook_notifications",
    log_prints=True,
    on_completion=[on_flow_completion],
    on_failure=[on_flow_failure],
)
def webhook_notifications_flow() -> None:
    """Run a pipeline with webhook notifications at each stage."""
    send_notification("pipeline.started", {"source": "webhook_demo"})
    result = process_data()
    send_notification("pipeline.completed", result)


if __name__ == "__main__":
    webhook_notifications_flow()
