"""Hello World.

The simplest possible Prefect flow: two tasks executed sequentially.

Airflow equivalent: BashOperator tasks with >> dependency.
Prefect approach:    @task functions called in order inside a @flow.
"""

import subprocess

from dotenv import load_dotenv
from prefect import flow, task


@task
def say_hello() -> str:
    """Print and return a greeting."""
    msg = "Hello from Prefect!"
    print(msg)
    return msg


@task
def print_date() -> str:
    """Run the 'date' command and return its output."""
    result = subprocess.run(["date"], capture_output=True, text=True, check=True)
    output = result.stdout.strip()
    print(output)
    return output


@flow(name="basics_hello_world", log_prints=True)
def hello_world() -> None:
    """Run say_hello then print_date — sequential by default."""
    say_hello()
    print_date()


if __name__ == "__main__":
    load_dotenv()
    hello_world()
