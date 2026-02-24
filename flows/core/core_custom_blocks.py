"""Custom Blocks.

Demonstrate defining and using custom Block classes for typed configuration.

Airflow equivalent: Custom connection types, configuration classes.
Prefect approach:    Subclass Block from prefect.blocks.core for typed,
                     reusable configuration objects.
"""

from prefect import flow, task
from prefect.blocks.core import Block


class DatabaseConfig(Block):
    """Configuration block for database connections.

    Attributes:
        host: Database server hostname.
        port: Database server port.
        database: Database name.
        username: Database username.
    """

    host: str = "localhost"
    port: int = 5432
    database: str = "mydb"
    username: str = "admin"


class NotificationConfig(Block):
    """Configuration block for notification settings.

    Attributes:
        channel: Notification channel (e.g. "slack", "email").
        recipient: Who receives the notification.
        enabled: Whether notifications are active.
    """

    channel: str = "slack"
    recipient: str = "#general"
    enabled: bool = True


@task
def connect_database(config: DatabaseConfig) -> str:
    """Simulate connecting to a database using the config block.

    Args:
        config: A DatabaseConfig block instance.

    Returns:
        A connection summary string.
    """
    conn_str = f"{config.username}@{config.host}:{config.port}/{config.database}"
    msg = f"Connected to database: {conn_str}"
    print(msg)
    return msg


@task
def send_notification(config: NotificationConfig, message: str) -> str:
    """Simulate sending a notification using the config block.

    Args:
        config: A NotificationConfig block instance.
        message: The notification message.

    Returns:
        A delivery summary string.
    """
    if not config.enabled:
        msg = f"Notifications disabled — skipping: {message}"
        print(msg)
        return msg
    msg = f"Sent to {config.channel} ({config.recipient}): {message}"
    print(msg)
    return msg


@flow(name="core_custom_blocks", log_prints=True)
def custom_blocks_flow() -> None:
    """Demonstrate custom blocks for typed configuration.

    Blocks are constructed directly here for local testability.
    In production, blocks would be saved and loaded from the Prefect
    server with Block.save() and Block.load().
    """
    db_config = DatabaseConfig(host="db.example.com", port=5432, database="analytics", username="etl_user")
    notif_config = NotificationConfig(channel="slack", recipient="#data-alerts", enabled=True)

    connect_database(db_config)
    send_notification(notif_config, "Pipeline completed successfully")

    # Demonstrate disabled notifications
    disabled_config = NotificationConfig(enabled=False)
    send_notification(disabled_config, "This will be skipped")


if __name__ == "__main__":
    custom_blocks_flow()
