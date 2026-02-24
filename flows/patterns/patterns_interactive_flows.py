"""Interactive Flows.

Simulate human-in-the-loop approval patterns.

Airflow equivalent: Human-in-the-loop operators (DAG 106).
Prefect approach:    pause_flow_run() requires a server; here we simulate
                     the pattern with a mock approval step for local testing.
"""

from prefect import flow, task

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def prepare_data() -> dict:
    """Prepare data that requires human approval before publishing.

    Returns:
        A dict with the prepared data and metadata.
    """
    data = {
        "report": "Q4 Financial Summary",
        "total_revenue": 1_250_000,
        "total_expenses": 980_000,
        "requires_approval": True,
    }
    print(f"Prepared: {data['report']}")
    return data


@task
def mock_approval(data: dict) -> bool:
    """Simulate a human approval step.

    In production, use prefect.flow_runs.pause_flow_run() to pause
    and wait for a human to approve via the Prefect UI. Here we
    auto-approve for local testing.

    Args:
        data: The data awaiting approval.

    Returns:
        True if approved, False if rejected.
    """
    approved = data.get("total_revenue", 0) > data.get("total_expenses", 0)
    status = "APPROVED" if approved else "REJECTED"
    print(f"Approval decision for '{data['report']}': {status}")
    return approved


@task
def publish(data: dict) -> str:
    """Publish approved data.

    Args:
        data: The approved data to publish.

    Returns:
        A confirmation message.
    """
    msg = f"Published: {data['report']}"
    print(msg)
    return msg


@task
def archive(data: dict) -> str:
    """Archive rejected data.

    Args:
        data: The rejected data to archive.

    Returns:
        An archive confirmation message.
    """
    msg = f"Archived (rejected): {data['report']}"
    print(msg)
    return msg


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="patterns_interactive_flows", log_prints=True)
def interactive_flows_flow() -> None:
    """Demonstrate a human-in-the-loop approval pattern.

    Note: In production, replace mock_approval with pause_flow_run()
    to pause execution and wait for human input via the Prefect UI.
    """
    data = prepare_data()
    approved = mock_approval(data)

    if approved:
        publish(data)
    else:
        archive(data)


if __name__ == "__main__":
    interactive_flows_flow()
