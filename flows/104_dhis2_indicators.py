"""104 -- DHIS2 Indicators API.

Fetches indicators from the DHIS2 play server, parses numerator and
denominator expressions with regex to count operands, and computes an
expression complexity score with binning.

Airflow equivalent: DHIS2 indicator fetch (DAG 060).
Prefect approach:    Custom block auth, regex operand parsing, complexity bins.
"""

from __future__ import annotations

import csv
import importlib.util
import re
import sys
from pathlib import Path

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel

# Import shared helpers
_spec = importlib.util.spec_from_file_location(
    "_dhis2_helpers",
    Path(__file__).resolve().parent / "_dhis2_helpers.py",
)
assert _spec and _spec.loader
_helpers = importlib.util.module_from_spec(_spec)
sys.modules.setdefault("_dhis2_helpers", _helpers)
_spec.loader.exec_module(_helpers)

Dhis2Connection = _helpers.Dhis2Connection
get_dhis2_connection = _helpers.get_dhis2_connection
get_dhis2_password = _helpers.get_dhis2_password
fetch_metadata = _helpers.fetch_metadata
OPERAND_PATTERN = _helpers.OPERAND_PATTERN

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class FlatIndicator(BaseModel):
    """Flattened indicator with expression analysis."""

    id: str
    name: str
    short_name: str
    indicator_type_id: str
    indicator_type_name: str
    numerator_operands: int
    denominator_operands: int
    expression_complexity: int
    complexity_bin: str


class IndicatorReport(BaseModel):
    """Summary report for indicators."""

    total: int
    complexity_distribution: dict[str, int]
    most_complex_name: str
    simplest_name: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _count_operands(expression: str) -> int:
    """Count ``#{uid.uid}`` operands in a DHIS2 expression."""
    if not isinstance(expression, str):
        return 0
    return len(re.findall(OPERAND_PATTERN, expression))


def _count_operators(expression: str) -> int:
    """Count arithmetic operators in an expression."""
    if not isinstance(expression, str):
        return 0
    return sum(1 for c in expression if c in "+-*/")


def _complexity_bin(score: int) -> str:
    """Bin a complexity score into a category."""
    if score <= 1:
        return "trivial"
    elif score <= 3:
        return "simple"
    elif score <= 6:
        return "moderate"
    else:
        return "complex"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def fetch_indicators(conn: Dhis2Connection, password: str) -> list[dict]:
    """Fetch all indicators from DHIS2.

    Args:
        conn: DHIS2 connection block.
        password: DHIS2 password.

    Returns:
        List of raw indicator dicts.
    """
    records = fetch_metadata(conn, "indicators", password)
    print(f"Fetched {len(records)} indicators")
    return records


@task
def flatten_indicators(raw: list[dict]) -> list[FlatIndicator]:
    """Parse and flatten indicator expressions.

    For each indicator, count operands in numerator and denominator using
    ``re.findall(r"#\\{[^}]+\\}")``, count operators, compute a combined
    complexity score, and bin it.

    Args:
        raw: Raw indicator dicts from the API.

    Returns:
        List of FlatIndicator.
    """
    flat: list[FlatIndicator] = []
    for r in raw:
        numerator = r.get("numerator", "")
        denominator = r.get("denominator", "")
        num_ops = _count_operands(numerator)
        den_ops = _count_operands(denominator)
        num_operators = _count_operators(numerator) + _count_operators(denominator)
        complexity = num_ops + den_ops + num_operators
        indicator_type = r.get("indicatorType")
        flat.append(
            FlatIndicator(
                id=r["id"],
                name=r.get("name", ""),
                short_name=r.get("shortName", ""),
                indicator_type_id=indicator_type["id"] if isinstance(indicator_type, dict) else "",
                indicator_type_name=indicator_type.get("name", "") if isinstance(indicator_type, dict) else "",
                numerator_operands=num_ops,
                denominator_operands=den_ops,
                expression_complexity=complexity,
                complexity_bin=_complexity_bin(complexity),
            )
        )
    print(f"Flattened {len(flat)} indicators")
    return flat


@task
def write_indicator_csv(indicators: list[FlatIndicator], output_dir: str) -> Path:
    """Write flattened indicators to CSV.

    Args:
        indicators: Flattened indicators.
        output_dir: Output directory path.

    Returns:
        Path to the CSV file.
    """
    path = Path(output_dir) / "indicators.csv"
    fieldnames = list(FlatIndicator.model_fields.keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for ind in indicators:
            writer.writerow(ind.model_dump())
    print(f"Wrote {len(indicators)} indicators to {path}")
    return path


@task
def indicator_report(indicators: list[FlatIndicator]) -> IndicatorReport:
    """Build a summary report for indicators.

    Args:
        indicators: Flattened indicators.

    Returns:
        IndicatorReport.
    """
    dist: dict[str, int] = {}
    for ind in indicators:
        dist[ind.complexity_bin] = dist.get(ind.complexity_bin, 0) + 1
    most = max(indicators, key=lambda i: i.expression_complexity) if indicators else None
    least = min(indicators, key=lambda i: i.expression_complexity) if indicators else None
    return IndicatorReport(
        total=len(indicators),
        complexity_distribution=dist,
        most_complex_name=most.name if most else "",
        simplest_name=least.name if least else "",
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="104_dhis2_indicators", log_prints=True)
def dhis2_indicators_flow(output_dir: str | None = None) -> IndicatorReport:
    """Fetch, parse, and export DHIS2 indicators.

    Args:
        output_dir: Output directory. Uses temp dir if not provided.

    Returns:
        IndicatorReport.
    """
    if output_dir is None:
        import tempfile

        output_dir = tempfile.mkdtemp(prefix="dhis2_indicators_")

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    conn = get_dhis2_connection()
    password = get_dhis2_password()

    raw = fetch_indicators(conn, password)
    flat = flatten_indicators(raw)
    write_indicator_csv(flat, output_dir)
    report = indicator_report(flat)

    create_markdown_artifact(
        key="dhis2-indicator-report",
        markdown=(
            f"## Indicator Report\n\n"
            f"- Total: {report.total}\n"
            f"- Most complex: {report.most_complex_name}\n"
            f"- Complexity distribution: {report.complexity_distribution}\n"
        ),
    )
    print(f"Indicator report: {report.total} indicators")
    return report


if __name__ == "__main__":
    dhis2_indicators_flow()
