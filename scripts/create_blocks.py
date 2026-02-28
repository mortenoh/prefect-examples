"""Create DHIS2 credentials blocks for all known instances.

Saves (or overwrites) a Dhis2Credentials block for each DHIS2 instance.
Requires a running Prefect server (PREFECT_API_URL).

The default "dhis2" block is created from environment variables when set:

    DHIS2_BASE_URL  -- DHIS2 instance base URL
    DHIS2_USERNAME  -- DHIS2 username
    DHIS2_PASSWORD  -- DHIS2 password

If the env vars are absent the default block points to the play server.

Usage:
    PREFECT_API_URL=http://localhost:4200/api uv run python scripts/create_blocks.py
"""

from __future__ import annotations

import os

from prefect_dhis2.credentials import Dhis2Credentials

INSTANCES: list[dict[str, str]] = [
    {
        "name": "dhis2-dev",
        "base_url": "https://play.im.dhis2.org/dev",
        "username": "admin",
        "password": "district",
    },
    {
        "name": "dhis2-v42",
        "base_url": "https://play.im.dhis2.org/stable-2-42-4",
        "username": "admin",
        "password": "district",
    },
    {
        "name": "dhis2-v41",
        "base_url": "https://play.im.dhis2.org/stable-2-41-7",
        "username": "admin",
        "password": "district",
    },
    {
        "name": "dhis2-v40",
        "base_url": "https://play.im.dhis2.org/stable-2-40-11",
        "username": "admin",
        "password": "district",
    },
]


def main() -> None:
    # Default "dhis2" block -- from env vars or play server fallback
    base_url = os.environ.get("DHIS2_BASE_URL", "https://play.im.dhis2.org/dev")
    username = os.environ.get("DHIS2_USERNAME", "admin")
    password = os.environ.get("DHIS2_PASSWORD", "district")

    default_block = Dhis2Credentials(
        base_url=base_url,
        username=username,
        password=password,
    )
    default_block.save("dhis2", overwrite=True)
    print(f"Saved block: dhis2 -> {base_url}")

    for inst in INSTANCES:
        block = Dhis2Credentials(
            base_url=inst["base_url"],
            username=inst["username"],
            password=inst["password"],
        )
        block.save(inst["name"], overwrite=True)
        print(f"Saved block: {inst['name']} -> {inst['base_url']}")

    print(f"\nCreated {len(INSTANCES) + 1} DHIS2 credentials blocks.")


if __name__ == "__main__":
    main()
