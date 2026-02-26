"""Variables and Params.

Store and retrieve runtime configuration with Prefect Variables.

Airflow equivalent: Variables + params.
Prefect approach:    Variable.get()/set() + flow parameters.
"""

from __future__ import annotations

import json

from dotenv import load_dotenv
from prefect import flow, task
from prefect.variables import Variable
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class AppConfig(BaseModel):
    """Application configuration loaded from a Prefect Variable."""

    debug: bool = False
    batch_size: int = 10


@task
def read_config() -> AppConfig:
    """Set and retrieve an example config variable.

    Writes a JSON config to a Prefect Variable, reads it back, and
    returns the parsed model.

    Returns:
        Parsed AppConfig.
    """
    Variable.set("example_config", '{"debug": true, "batch_size": 100}', overwrite=True)
    raw = Variable.get("example_config", default="{}")
    config = AppConfig(**json.loads(str(raw)))
    print(f"Loaded config: {config}")
    return config


@task
def process_with_config(config: AppConfig, env: str) -> str:
    """Use the configuration to drive processing behaviour.

    Args:
        config: Application configuration.
        env: The target environment name.

    Returns:
        A summary string describing the processing parameters.
    """
    summary = f"env={env}, debug={config.debug}, batch_size={config.batch_size}"
    print(f"Processing with: {summary}")
    return summary


@flow(name="basics_variables_and_params", log_prints=True)
def variables_flow(environment: str = "dev") -> None:
    """Read config from Variables and process with the given environment.

    Args:
        environment: Target environment name (default: "dev").
    """
    config = read_config()
    process_with_config(config, environment)


if __name__ == "__main__":
    load_dotenv()
    variables_flow()
