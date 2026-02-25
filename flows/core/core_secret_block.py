"""Secret Block.

Demonstrate using Prefect's Secret block for secure credential management.

Airflow equivalent: Connections / Variables with is_encrypted.
Prefect approach:    Secret block from prefect.blocks.system with graceful
                     fallback for local development.
"""

from typing import Any

from prefect import flow, task
from prefect.blocks.system import Secret


@task
def get_api_key() -> str:
    """Retrieve an API key from a Secret block.

    Attempts to load the secret from the Prefect server. Falls back
    to a development key when the block is not registered (e.g. in
    local development or testing).

    Returns:
        The API key string.
    """
    try:
        secret = Secret.load("example-api-key")
        api_key: str = secret.get()  # type: ignore[union-attr]
        print("Loaded API key from Secret block")
    except ValueError:
        api_key = "dev-fallback-key-12345"
        print("Secret block not found — using fallback key")
    return api_key


@task
def call_api(api_key: str, endpoint: str) -> dict[str, Any]:
    """Simulate an API call using the provided key.

    Args:
        api_key: The API key for authentication.
        endpoint: The API endpoint to call.

    Returns:
        A dict with the simulated API response.
    """
    # Mask the key in logs
    masked = api_key[:4] + "****" + api_key[-4:]
    print(f"Calling {endpoint} with key {masked}")
    result = {
        "endpoint": endpoint,
        "status": 200,
        "data": {"message": "Success"},
        "authenticated": not api_key.startswith("dev-fallback"),
    }
    print(f"API response: {result['status']}")
    return result


@flow(name="core_secret_block", log_prints=True)
def secret_block_flow() -> None:
    """Retrieve a secret and use it to call an API."""
    api_key = get_api_key()
    call_api(api_key, "/api/v1/data")
    call_api(api_key, "/api/v1/status")


if __name__ == "__main__":
    secret_block_flow()
