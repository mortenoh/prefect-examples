"""Quality Rules Engine.

Configuration-driven data quality rules with traffic-light scoring. Define
rules as config dicts, execute them against data, and compute an overall
quality score with green/amber/red classification.

Airflow equivalent: Freshness and completeness checks (DAG 070).
Prefect approach:    Rule registry pattern with @task dispatchers and
                     Pydantic models for type-safe results.
"""

from typing import Any

from dotenv import load_dotenv
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class QualityRule(BaseModel):
    """A single quality rule definition."""

    name: str
    rule_type: str
    column: str = ""
    params: dict[str, Any] = {}


class RuleResult(BaseModel):
    """Result of executing a quality rule."""

    rule_name: str
    rule_type: str
    passed: bool
    score: float
    severity: str = "error"
    details: str = ""


class QualityReport(BaseModel):
    """Overall quality report with traffic-light scoring."""

    overall_score: float
    traffic_light: str
    total_rules: int
    passed_rules: int
    failed_rules: int
    results: list[RuleResult]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def build_rules(config: list[dict[str, Any]]) -> list[QualityRule]:
    """Build quality rules from configuration dicts.

    Args:
        config: List of rule configuration dicts.

    Returns:
        List of QualityRule objects.
    """
    rules = [QualityRule(**c) for c in config]
    print(f"Built {len(rules)} quality rules")
    return rules


@task
def generate_sample_data(rows: int = 20) -> list[dict[str, Any]]:
    """Generate sample data for quality checking.

    Args:
        rows: Number of rows.

    Returns:
        List of record dicts.
    """
    data = []
    for i in range(1, rows + 1):
        record: dict[str, Any] = {
            "id": i,
            "name": f"item_{i}" if i % 10 != 0 else "",
            "value": float(i * 10) if i % 15 != 0 else -1.0,
            "category": ["A", "B", "C"][i % 3],
        }
        data.append(record)
    return data


@task
def run_not_null_check(data: list[dict[str, Any]], column: str) -> RuleResult:
    """Check that a column has no null/empty values.

    Args:
        data: Dataset rows.
        column: Column to check.

    Returns:
        RuleResult with pass/fail and null rate.
    """
    total = len(data)
    nulls = sum(1 for row in data if not row.get(column))
    null_rate = nulls / total if total > 0 else 0.0
    passed = nulls == 0
    return RuleResult(
        rule_name=f"not_null_{column}",
        rule_type="not_null",
        passed=passed,
        score=1.0 - null_rate,
        details=f"{nulls}/{total} null values ({null_rate:.1%})",
    )


@task
def run_range_check(data: list[dict[str, Any]], column: str, min_val: float, max_val: float) -> RuleResult:
    """Check that numeric values fall within a range.

    Args:
        data: Dataset rows.
        column: Column to check.
        min_val: Minimum acceptable value.
        max_val: Maximum acceptable value.

    Returns:
        RuleResult with pass/fail and out-of-range count.
    """
    total = len(data)
    out_of_range = sum(
        1
        for row in data
        if isinstance(row.get(column), (int, float)) and (row[column] < min_val or row[column] > max_val)
    )
    rate = out_of_range / total if total > 0 else 0.0
    passed = out_of_range == 0
    return RuleResult(
        rule_name=f"range_{column}",
        rule_type="range",
        passed=passed,
        score=1.0 - rate,
        details=f"{out_of_range}/{total} out of range [{min_val}, {max_val}]",
    )


@task
def run_uniqueness_check(data: list[dict[str, Any]], column: str) -> RuleResult:
    """Check that values in a column are unique.

    Args:
        data: Dataset rows.
        column: Column to check.

    Returns:
        RuleResult with duplicate count.
    """
    values = [row.get(column) for row in data]
    total = len(values)
    unique = len(set(values))
    duplicates = total - unique
    passed = duplicates == 0
    return RuleResult(
        rule_name=f"unique_{column}",
        rule_type="uniqueness",
        passed=passed,
        score=unique / total if total > 0 else 1.0,
        details=f"{duplicates} duplicates out of {total} values",
    )


@task
def run_completeness_check(data: list[dict[str, Any]], expected_count: int) -> RuleResult:
    """Check that the dataset has the expected number of rows.

    Args:
        data: Dataset rows.
        expected_count: Expected minimum row count.

    Returns:
        RuleResult with completeness ratio.
    """
    actual = len(data)
    ratio = actual / expected_count if expected_count > 0 else 0.0
    passed = actual >= expected_count
    return RuleResult(
        rule_name="completeness",
        rule_type="completeness",
        passed=passed,
        score=min(ratio, 1.0),
        details=f"{actual}/{expected_count} rows present",
    )


@task
def execute_rule(data: list[dict[str, Any]], rule: QualityRule) -> RuleResult:
    """Dispatch and execute a quality rule.

    Args:
        data: Dataset rows.
        rule: The rule to execute.

    Returns:
        RuleResult from the appropriate check.
    """
    if rule.rule_type == "not_null":
        return run_not_null_check.fn(data, rule.column)
    elif rule.rule_type == "range":
        return run_range_check.fn(
            data,
            rule.column,
            rule.params.get("min", 0),
            rule.params.get("max", 1000),
        )
    elif rule.rule_type == "uniqueness":
        return run_uniqueness_check.fn(data, rule.column)
    elif rule.rule_type == "completeness":
        return run_completeness_check.fn(data, rule.params.get("expected_count", 0))
    else:
        return RuleResult(
            rule_name=rule.name,
            rule_type=rule.rule_type,
            passed=False,
            score=0.0,
            details=f"Unknown rule type: {rule.rule_type}",
        )


@task
def compute_quality_score(results: list[RuleResult]) -> QualityReport:
    """Compute overall quality score and traffic light from rule results.

    Args:
        results: List of individual rule results.

    Returns:
        QualityReport with overall score and traffic light.
    """
    if not results:
        return QualityReport(
            overall_score=0.0,
            traffic_light="red",
            total_rules=0,
            passed_rules=0,
            failed_rules=0,
            results=[],
        )
    overall = sum(r.score for r in results) / len(results)
    passed = sum(1 for r in results if r.passed)
    failed = len(results) - passed

    if overall >= 0.9:
        traffic_light = "green"
    elif overall >= 0.7:
        traffic_light = "amber"
    else:
        traffic_light = "red"

    return QualityReport(
        overall_score=round(overall, 4),
        traffic_light=traffic_light,
        total_rules=len(results),
        passed_rules=passed,
        failed_rules=failed,
        results=results,
    )


@task
def publish_quality_report(report: QualityReport) -> str:
    """Publish a markdown quality report artifact.

    Args:
        report: The quality report.

    Returns:
        Markdown string.
    """
    lines = [
        f"# Quality Report [{report.traffic_light.upper()}]",
        "",
        f"**Overall Score:** {report.overall_score:.1%}",
        f"**Rules:** {report.passed_rules}/{report.total_rules} passed",
        "",
        "| Rule | Type | Score | Passed | Details |",
        "|---|---|---|---|---|",
    ]
    for r in report.results:
        status = "PASS" if r.passed else "FAIL"
        lines.append(f"| {r.rule_name} | {r.rule_type} | {r.score:.2f} | {status} | {r.details} |")

    markdown = "\n".join(lines)
    create_markdown_artifact(
        key="quality-report",
        markdown=markdown,
        description="Data quality report",
    )
    return markdown


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="data_engineering_quality_rules_engine", log_prints=True)
def quality_rules_engine_flow(rules_config: list[dict[str, Any]] | None = None) -> QualityReport:
    """Run a configuration-driven quality rules engine.

    Args:
        rules_config: Optional list of rule config dicts. Uses defaults if None.

    Returns:
        Quality report with traffic-light score.
    """
    if rules_config is None:
        rules_config = [
            {"name": "id_not_null", "rule_type": "not_null", "column": "id"},
            {"name": "name_not_null", "rule_type": "not_null", "column": "name"},
            {"name": "value_range", "rule_type": "range", "column": "value", "params": {"min": 0, "max": 500}},
            {"name": "id_unique", "rule_type": "uniqueness", "column": "id"},
            {"name": "row_count", "rule_type": "completeness", "params": {"expected_count": 15}},
        ]

    rules = build_rules(rules_config)
    data = generate_sample_data(rows=20)

    results = [execute_rule(data, rule) for rule in rules]

    report = compute_quality_score(results)
    publish_quality_report(report)

    print(f"Quality: {report.traffic_light} ({report.overall_score:.1%})")
    return report


if __name__ == "__main__":
    load_dotenv()
    quality_rules_engine_flow()
