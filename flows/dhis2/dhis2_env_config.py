"""DHIS2 Environment-Based Configuration.

Multiple configuration strategies in one flow: inline blocks, Secret blocks,
environment variables, JSON config. Shows the Prefect equivalents of all
Airflow configuration mechanisms.

Airflow equivalent: None (Prefect-native pattern).
Prefect approach:    Compare inline, Secret, env var, and JSON block strategies.
"""

from __future__ import annotations

import json
import os

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class ConfigSource(BaseModel):
    """A single configuration value and its source."""

    source_type: str
    key: str
    value: str
    is_secret: bool


class ConfigReport(BaseModel):
    """Summary of available configuration strategies."""

    sources: list[ConfigSource]
    strategy_count: int


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def from_inline_block() -> ConfigSource:
    """Load configuration from an inline block (hardcoded defaults).

    Returns:
        ConfigSource.
    """
    source = ConfigSource(
        source_type="inline_block",
        key="base_url",
        value="https://play.im.dhis2.org/dev",
        is_secret=False,
    )
    print(f"Inline block: {source.key}={source.value}")
    return source


@task
def from_secret_block() -> ConfigSource:
    """Load a secret from a Prefect Secret block with fallback.

    Returns:
        ConfigSource.
    """
    try:
        from prefect.blocks.system import Secret

        secret = Secret.load("dhis2-password")  # type: ignore[union-attr]
        value = secret.get()  # type: ignore[union-attr]
    except Exception:
        value = "district"
    source = ConfigSource(
        source_type="secret_block",
        key="password",
        value="***" if value else "",
        is_secret=True,
    )
    print(f"Secret block: {source.key}={'***' if source.is_secret else source.value}")
    return source


@task
def from_env_var(key: str, default: str) -> ConfigSource:
    """Load configuration from an environment variable.

    Args:
        key: Environment variable name.
        default: Default value if not set.

    Returns:
        ConfigSource.
    """
    value = os.environ.get(key, default)
    source = ConfigSource(
        source_type="env_var",
        key=key,
        value=value,
        is_secret=False,
    )
    print(f"Env var: {source.key}={source.value}")
    return source


@task
def from_json_config(config_json: str | None = None) -> ConfigSource:
    """Load configuration from a JSON string (simulating JSON block).

    In a real deployment, this would use ``JSON.load("dhis2-config")``.

    Args:
        config_json: JSON config string. Uses default if not provided.

    Returns:
        ConfigSource.
    """
    if config_json is None:
        config_json = json.dumps({"base_url": "https://play.im.dhis2.org/dev", "api_version": "43"})
    config = json.loads(config_json)
    value = config.get("base_url", "")
    source = ConfigSource(
        source_type="json_block",
        key="base_url",
        value=value,
        is_secret=False,
    )
    print(f"JSON block: {source.key}={source.value}")
    return source


@task
def compare_strategies(sources: list[ConfigSource]) -> ConfigReport:
    """Compare configuration strategies and report availability.

    Args:
        sources: Config sources collected from each strategy.

    Returns:
        ConfigReport.
    """
    unique_types = {s.source_type for s in sources}
    report = ConfigReport(sources=sources, strategy_count=len(unique_types))
    print(f"Compared {report.strategy_count} configuration strategies")
    return report


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="dhis2_env_config", log_prints=True)
def dhis2_env_config_flow() -> ConfigReport:
    """Demonstrate multiple configuration strategies for DHIS2 integration.

    Returns:
        ConfigReport.
    """
    sources: list[ConfigSource] = []
    sources.append(from_inline_block())
    sources.append(from_secret_block())
    sources.append(from_env_var("DHIS2_BASE_URL", "https://play.im.dhis2.org/dev"))
    sources.append(from_json_config())

    report = compare_strategies(sources)

    lines = ["## Configuration Strategies", ""]
    lines.append("| Strategy | Key | Secret |")
    lines.append("|----------|-----|--------|")
    for s in report.sources:
        lines.append(f"| {s.source_type} | {s.key} | {'yes' if s.is_secret else 'no'} |")
    create_markdown_artifact(key="dhis2-config-report", markdown="\n".join(lines))

    print(f"Config report: {report.strategy_count} strategies available")
    return report


if __name__ == "__main__":
    dhis2_env_config_flow()
