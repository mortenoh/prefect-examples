"""Notification Blocks.

Use Prefect built-in notification blocks for pipeline alerting.

Airflow equivalent: Slack/email/PagerDuty callbacks via operators.
Prefect approach:    NotificationBlock subclasses with unified notify(body, subject).
"""

import os
from typing import Any

from dotenv import load_dotenv
from prefect import flow, task
from prefect.blocks.notifications import CustomWebhookNotificationBlock, SlackWebhook
from prefect.types import SecretDict
from pydantic import SecretStr

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def configure_notification_blocks() -> dict[str, Any]:
    """Construct notification blocks programmatically.

    Demonstrates the constructor API for SlackWebhook and
    CustomWebhookNotificationBlock.  In production you would persist
    these with ``block.save("my-block")`` and retrieve them with
    ``SlackWebhook.load("my-block")``.

    Returns:
        Summary dict describing the configured channels.
    """
    # SlackWebhook -- url must be a SecretStr
    slack_webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T00/B00/xxxx")
    slack = SlackWebhook(url=SecretStr(slack_webhook_url))

    # CustomWebhookNotificationBlock -- flexible for any HTTP endpoint
    custom = CustomWebhookNotificationBlock(
        name="ops-webhook",
        url="https://monitoring.example.com/alerts",
        method="POST",
        json_data={"text": "{{subject}}: {{body}}"},
        headers=None,
        cookies=None,
        secrets=SecretDict({"api_token": "placeholder-token"}),
    )

    # In production:
    #   slack.save("prod-slack", overwrite=True)
    #   custom.save("ops-webhook", overwrite=True)

    summary = {
        "slack": {
            "type": type(slack).__name__,
            "notify_type": slack.notify_type,
        },
        "custom": {
            "type": type(custom).__name__,
            "name": custom.name,
            "method": custom.method,
        },
    }
    print(f"Configured {len(summary)} notification channels: {list(summary.keys())}")
    return summary


@task
def demonstrate_template_resolution() -> dict[str, Any]:
    """Show how CustomWebhookNotificationBlock resolves templates.

    Builds a block with ``{{subject}}``, ``{{body}}``, ``{{name}}``, and
    a custom ``{{api_token}}`` secret in both the URL and JSON payload.
    Calls ``_build_request_args(body, subject)`` to produce the fully
    resolved HTTP request without making any network call.

    Returns:
        The resolved request dict (method, url, json, headers, ...).
    """
    block = CustomWebhookNotificationBlock(
        name="template-demo",
        url="https://api.example.com/notify?token={{api_token}}",
        method="POST",
        json_data={
            "title": "{{subject}}",
            "message": "{{body}}",
            "source": "{{name}}",
            "auth": "Bearer {{api_token}}",
        },
        headers=None,
        cookies=None,
        secrets=SecretDict({"api_token": "secret-xyz-789"}),
    )

    resolved = block._build_request_args(
        body="Pipeline completed: 150 records processed",
        subject="Pipeline Alert",
    )

    print(f"Resolved URL: {resolved['url']}")
    print(f"Resolved JSON: {resolved['json']}")
    return resolved


@task
def process_data(source: str) -> dict[str, Any]:
    """Simulate processing records from a data source.

    Args:
        source: Name of the data source.

    Returns:
        A dict with source, records count, and status.
    """
    records = {"api": 150, "database": 320, "file": 45}.get(source, 10)
    result = {"source": source, "records": records, "status": "success"}
    print(f"Processed {records} records from {source}")
    return result


# ---------------------------------------------------------------------------
# Hooks
# ---------------------------------------------------------------------------


def on_completion_notify(flow: Any, flow_run: Any, state: Any) -> None:
    """Notify on flow completion.

    In production:
        SlackWebhook.load("prod-slack").notify(
            body=f"Flow {flow_run.name!r} completed.",
            subject="Flow Completed",
        )
    """
    print(f"HOOK  Flow {flow_run.name!r} completed -- would notify via SlackWebhook")


def on_failure_notify(flow: Any, flow_run: Any, state: Any) -> None:
    """Escalate on flow failure.

    In production:
        PagerDutyWebHook.load("oncall").notify(
            body=f"CRITICAL: Flow {flow_run.name!r} failed: {state.message}",
            subject="Flow Failed",
        )
    """
    print(f"HOOK  CRITICAL: Flow {flow_run.name!r} failed -- would escalate via PagerDuty")


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(
    name="patterns_notification_blocks",
    log_prints=True,
    on_completion=[on_completion_notify],
    on_failure=[on_failure_notify],
)
def notification_blocks_flow() -> None:
    """Run a pipeline with built-in notification block integration."""
    # Step 1 -- configure notification channels
    channels = configure_notification_blocks()
    print(f"Channels ready: {channels}")

    # Step 2 -- process data from multiple sources
    for source in ["api", "database", "file"]:
        process_data(source)

    # Step 3 -- demonstrate template resolution (no HTTP calls)
    resolved = demonstrate_template_resolution()
    print(f"Template resolution produced {len(resolved)} fields")


if __name__ == "__main__":
    load_dotenv()
    notification_blocks_flow()
