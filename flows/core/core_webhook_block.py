"""Webhook Block.

Demonstrate the built-in Webhook block for configurable outbound HTTP calls.

Airflow equivalent: SimpleHttpOperator with connection credentials.
Prefect approach:    Use the Webhook block from prefect.blocks.webhook to store
                     URL, method, headers, and auth in a reusable, server-persisted
                     block. The block powers the CallWebhook automation action and
                     replaces ad-hoc httpx.post() calls with managed configuration.

Note: Prefect Cloud also supports *inbound* webhooks -- external systems POST to
a Prefect-managed endpoint and a Jinja2 template transforms the payload into a
Prefect event. Inbound webhooks are managed via ``prefect webhook create/ls/...``
CLI commands and are not demonstrated here because they require Cloud.
"""

from typing import Any

from dotenv import load_dotenv
from prefect import flow, task
from prefect.blocks.webhook import Webhook
from prefect.types import SecretDict
from pydantic import SecretStr

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def create_get_webhook() -> dict[str, Any]:
    """Create a Webhook block configured for GET requests.

    Returns:
        Summary dict with block configuration details.
    """
    webhook = Webhook(
        method="GET",
        url=SecretStr("https://api.example.com/health"),
        headers=SecretDict({"Accept": "application/json"}),
    )
    summary = {
        "method": webhook.method,
        "url_host": "api.example.com",
        "header_keys": list(webhook.headers.get_secret_value().keys()),
    }
    print(f"GET webhook configured: {summary}")
    return summary


@task
def create_post_webhook() -> dict[str, Any]:
    """Create a Webhook block configured for POST requests with auth.

    Returns:
        Summary dict with block configuration details.
    """
    webhook = Webhook(
        method="POST",
        url=SecretStr("https://api.example.com/events"),
        headers=SecretDict(
            {
                "Content-Type": "application/json",
                "Authorization": "Bearer placeholder-token",
            }
        ),
    )
    summary = {
        "method": webhook.method,
        "url_host": "api.example.com",
        "header_keys": list(webhook.headers.get_secret_value().keys()),
    }
    print(f"POST webhook configured: {summary}")
    return summary


@task
def simulate_webhook_call(method: str, url_host: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Simulate calling a webhook without making a real HTTP request.

    In production you would call ``await webhook.call(payload)`` which returns
    an httpx Response. Here we simulate the call to avoid requiring external
    connectivity.

    Args:
        method: HTTP method (GET, POST, etc.).
        url_host: Target host for logging.
        payload: Optional JSON payload for POST/PUT requests.

    Returns:
        Dict summarising what would have been sent.
    """
    result: dict[str, Any] = {
        "method": method,
        "url_host": url_host,
        "payload": payload,
        "simulated": True,
    }
    print(f"Simulated {method} to {url_host} -- payload: {payload}")
    return result


@task
def demonstrate_save_load_pattern() -> dict[str, str]:
    """Show the save/load pattern for block persistence.

    Blocks can be saved to a Prefect server and loaded by name in other flows
    or deployments. Without a running server, save() will fail gracefully.

    Returns:
        Dict describing the pattern.
    """
    Webhook(
        method="POST",
        url=SecretStr("https://hooks.example.com/ingest"),
        headers=SecretDict({"X-API-Key": "secret-key-value"}),
    )

    # In production with a Prefect server:
    #   webhook.save("my-webhook", overwrite=True)
    #   loaded = Webhook.load("my-webhook")
    #   response = await loaded.call({"event": "deploy.completed"})

    pattern = {
        "save": 'webhook.save("my-webhook", overwrite=True)',
        "load": 'Webhook.load("my-webhook")',
        "call": 'await webhook.call({"event": "deploy.completed"})',
    }
    print(f"Save/load pattern: {pattern}")
    return pattern


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="core_webhook_block", log_prints=True)
def webhook_block_flow() -> dict[str, Any]:
    """Demonstrate the Webhook block for outbound HTTP calls.

    Creates GET and POST webhook configurations, simulates calling them,
    and shows the save/load pattern for server persistence.
    """
    # Configure webhook blocks
    get_summary = create_get_webhook()
    post_summary = create_post_webhook()

    # Simulate calls
    get_result = simulate_webhook_call(
        method=get_summary["method"],
        url_host=get_summary["url_host"],
    )
    post_result = simulate_webhook_call(
        method=post_summary["method"],
        url_host=post_summary["url_host"],
        payload={"event": "pipeline.completed", "records": 150},
    )

    # Show persistence pattern
    pattern = demonstrate_save_load_pattern()

    return {
        "get_result": get_result,
        "post_result": post_result,
        "persistence_pattern": pattern,
    }


if __name__ == "__main__":
    load_dotenv()
    webhook_block_flow()
