"""Parameterized Flows.

Accept runtime parameters with typed defaults.

Airflow equivalent: Jinja2 templating / params dict.
Prefect approach:    typed function parameters with defaults.
"""

import datetime

from prefect import flow, task


@task
def build_greeting(name: str, date_str: str, template: str) -> str:
    """Format a greeting from the given template.

    Args:
        name: The recipient name.
        date_str: A date string to embed in the greeting.
        template: A format string with {name} and {date} placeholders.

    Returns:
        The fully rendered greeting.
    """
    msg = template.format(name=name, date=date_str)
    print(msg)
    return msg


@flow(name="basics_parameterized_flows", log_prints=True)
def parameterized_flow(
    name: str = "World",
    date_str: str | None = None,
    template: str = "Greetings, {name}! Today is {date}.",
) -> None:
    """Run a greeting task using caller-supplied parameters.

    Args:
        name: The recipient name.
        date_str: Optional date string; defaults to today's date.
        template: Greeting template with {name} and {date} placeholders.
    """
    if date_str is None:
        date_str = datetime.date.today().isoformat()

    build_greeting(name, date_str, template)


if __name__ == "__main__":
    parameterized_flow()
