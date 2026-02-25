"""Create DHIS2 credentials blocks for all known instances.

Saves (or overwrites) a Dhis2Credentials block for each DHIS2 instance.
Requires a running Prefect server (PREFECT_API_URL).

Usage:
    PREFECT_API_URL=http://localhost:4200/api uv run python scripts/create_blocks.py
"""

from __future__ import annotations

import asyncio

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


async def main() -> None:
    for inst in INSTANCES:
        block = Dhis2Credentials(
            base_url=inst["base_url"],
            username=inst["username"],
            password=inst["password"],
        )
        await block.save(inst["name"], overwrite=True)
        print(f"Saved block: {inst['name']} -> {inst['base_url']}")

    print(f"\nCreated {len(INSTANCES)} DHIS2 credentials blocks.")


if __name__ == "__main__":
    asyncio.run(main())
