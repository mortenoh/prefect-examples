"""094 -- Expression Complexity Scoring.

Regex-based expression parsing, operand counting, complexity scoring
and binning, and metadata categorization.

Airflow equivalent: DHIS2 indicator expression parsing (DAGs 059-060).
Prefect approach:    Simulate expressions with #{...} operands, parse with
                     regex, count operators, score and bin complexity.
"""

import re

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class Expression(BaseModel):
    """An indicator expression with metadata."""

    id: str
    name: str
    numerator: str
    denominator: str
    category: str


class ComplexityScore(BaseModel):
    """Complexity score for an expression."""

    id: str
    operand_count: int
    operator_count: int
    score: int
    complexity_bin: str


class ScoringReport(BaseModel):
    """Summary scoring report."""

    total: int
    complexity_distribution: dict[str, int]
    most_complex: str
    simplest: str


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OPERAND_PATTERN = re.compile(r"#\{[^}]+\}")

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def simulate_expressions() -> list[Expression]:
    """Generate deterministic indicator expressions.

    Returns:
        List of Expression.
    """
    data = [
        Expression(
            id="IND_001",
            name="ANC Coverage",
            numerator="#{ANC_visits}",
            denominator="#{expected_pregnancies}",
            category="Maternal Health",
        ),
        Expression(
            id="IND_002",
            name="Immunization Rate",
            numerator="#{fully_immunized} + #{partially_immunized}",
            denominator="#{target_population}",
            category="Child Health",
        ),
        Expression(
            id="IND_003",
            name="Treatment Success",
            numerator="(#{cured} + #{completed}) * 100",
            denominator="#{total_treated} - #{transferred_out} - #{died}",
            category="Disease Control",
        ),
        Expression(
            id="IND_004",
            name="Facility Utilization",
            numerator="#{outpatient_visits} + #{inpatient_days} + #{emergency_visits}",
            denominator="#{bed_capacity} * #{days_in_period}",
            category="Service Delivery",
        ),
        Expression(
            id="IND_005",
            name="Stock Coverage",
            numerator="#{stock_on_hand}",
            denominator="#{average_monthly_consumption}",
            category="Supply Chain",
        ),
        Expression(
            id="IND_006",
            name="Dropout Rate",
            numerator="#{dose1} - #{dose3}",
            denominator="#{dose1}",
            category="Child Health",
        ),
        Expression(
            id="IND_007",
            name="Composite Quality Index",
            numerator="(#{indicator_a} * #{weight_a} + #{indicator_b} * #{weight_b} + #{indicator_c} * #{weight_c})",
            denominator="#{weight_a} + #{weight_b} + #{weight_c}",
            category="Quality",
        ),
    ]
    print(f"Simulated {len(data)} expressions")
    return data


@task
def parse_operands(expression: str) -> int:
    """Count operands in an expression using regex.

    Args:
        expression: Expression string with #{...} operands.

    Returns:
        Number of operands found.
    """
    return len(OPERAND_PATTERN.findall(expression))


@task
def count_operators(expression: str) -> int:
    """Count arithmetic operators in an expression.

    Args:
        expression: Expression string.

    Returns:
        Number of operators.
    """
    operators = ["+", "-", "*", "/"]
    count = 0
    for char in expression:
        if char in operators:
            count += 1
    return count


@task
def score_complexity(expressions: list[Expression]) -> list[ComplexityScore]:
    """Score and bin complexity for each expression.

    Score = total operands (numerator + denominator) + total operators.
    Bins: trivial (<=2), simple (<=4), moderate (<=8), complex (>8).

    Args:
        expressions: List of expressions.

    Returns:
        List of ComplexityScore.
    """
    scores: list[ComplexityScore] = []
    for expr in expressions:
        num_operands = parse_operands.fn(expr.numerator) + parse_operands.fn(expr.denominator)
        num_operators = count_operators.fn(expr.numerator) + count_operators.fn(expr.denominator)
        total = num_operands + num_operators

        if total <= 2:
            bin_label = "trivial"
        elif total <= 4:
            bin_label = "simple"
        elif total <= 8:
            bin_label = "moderate"
        else:
            bin_label = "complex"

        scores.append(
            ComplexityScore(
                id=expr.id,
                operand_count=num_operands,
                operator_count=num_operators,
                score=total,
                complexity_bin=bin_label,
            )
        )
    return scores


@task
def categorize_by_domain(expressions: list[Expression]) -> dict[str, int]:
    """Count expressions by domain category.

    Args:
        expressions: List of expressions.

    Returns:
        Dict mapping category to count.
    """
    counts: dict[str, int] = {}
    for expr in expressions:
        counts[expr.category] = counts.get(expr.category, 0) + 1
    return counts


@task
def scoring_summary(scores: list[ComplexityScore]) -> ScoringReport:
    """Build the final scoring report.

    Args:
        scores: Complexity scores.

    Returns:
        ScoringReport.
    """
    dist: dict[str, int] = {}
    for s in scores:
        dist[s.complexity_bin] = dist.get(s.complexity_bin, 0) + 1

    sorted_scores = sorted(scores, key=lambda s: s.score)
    return ScoringReport(
        total=len(scores),
        complexity_distribution=dist,
        most_complex=sorted_scores[-1].id if sorted_scores else "",
        simplest=sorted_scores[0].id if sorted_scores else "",
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="094_expression_scoring", log_prints=True)
def expression_scoring_flow() -> ScoringReport:
    """Run the expression complexity scoring pipeline.

    Returns:
        ScoringReport.
    """
    expressions = simulate_expressions()
    scores = score_complexity(expressions)
    categories = categorize_by_domain(expressions)
    report = scoring_summary(scores)
    print(f"Scoring complete: {report.total} expressions, most complex: {report.most_complex}")
    print(f"Domain categories: {categories}")
    return report


if __name__ == "__main__":
    expression_scoring_flow()
