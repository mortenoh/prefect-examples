"""Variables and Params.

Store and retrieve runtime configuration with Prefect Variables.

Airflow equivalent: Variables + params.
Prefect approach:    Variable.get()/set() + flow parameters.
"""

import json

from prefect import flow, task
from prefect.variables import Variable


@task
def read_config() -> dict:
    """Set and retrieve an example config variable.

    Writes a JSON config to a Prefect Variable, reads it back, and
    returns the parsed dictionary.

    Returns:
        Parsed configuration dictionary.
    """
    Variable.set("example_config", '{"debug": true, "batch_size": 100}', overwrite=True)
    raw = Variable.get("example_config", default="{}")
    config = json.loads(str(raw))
    print(f"Loaded config: {config}")
    return config


@task
def process_with_config(config: dict, env: str) -> str:
    """Use the configuration to drive processing behaviour.

    Args:
        config: Configuration dictionary with keys like "debug" and "batch_size".
        env: The target environment name.

    Returns:
        A summary string describing the processing parameters.
    """
    summary = f"env={env}, debug={config.get('debug', False)}, batch_size={config.get('batch_size', 10)}"
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
    variables_flow()
