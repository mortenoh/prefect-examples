"""084 -- Statistical Aggregation.

Fan-out aggregation pattern: one dataset, multiple independent aggregations
running in parallel. Includes grouped statistics and cross-tabulation.

Airflow equivalent: Parquet-style aggregation with groupby and cross-tab (DAG 057).
Prefect approach:    Generate weather data, fan-out to 3 parallel aggregations
                     (by station, by date, cross-tab), then combine into a report.
"""

import statistics

from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class WeatherObservation(BaseModel):
    """A single weather observation."""

    station: str
    day: str
    temperature: float
    humidity: float


class GroupStats(BaseModel):
    """Statistics for a single group."""

    group_value: str
    mean: float
    min_val: float
    max_val: float
    count: int


class AggregationResult(BaseModel):
    """Result of a grouped aggregation."""

    group_key: str
    groups: list[GroupStats]
    metric_count: int


class CrossTab(BaseModel):
    """Cross-tabulation matrix."""

    row_labels: list[str]
    col_labels: list[str]
    matrix: list[list[float]]


class AggregationReport(BaseModel):
    """Combined aggregation report."""

    by_station: AggregationResult
    by_date: AggregationResult
    cross_tab: CrossTab
    record_count: int


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def generate_weather_data(stations: list[str], days: int) -> list[WeatherObservation]:
    """Generate deterministic weather data.

    Args:
        stations: Station names.
        days: Number of days.

    Returns:
        List of WeatherObservation.
    """
    records: list[WeatherObservation] = []
    for si, station in enumerate(stations):
        for day in range(1, days + 1):
            temp = round(15.0 + si * 3.0 + (day * 1.7) % 10.0, 1)
            humidity = round(40.0 + (si * 7 + day * 3) % 40, 1)
            records.append(
                WeatherObservation(
                    station=station,
                    day=f"day_{day:02d}",
                    temperature=temp,
                    humidity=humidity,
                )
            )
    print(f"Generated {len(records)} weather records")
    return records


@task
def aggregate_by_group(records: list[WeatherObservation], group_field: str, value_field: str) -> AggregationResult:
    """Compute grouped statistics.

    Args:
        records: Input records.
        group_field: Field to group by.
        value_field: Numeric field to aggregate.

    Returns:
        AggregationResult with per-group stats.
    """
    groups: dict[str, list[float]] = {}
    for r in records:
        key = str(getattr(r, group_field))
        groups.setdefault(key, []).append(float(getattr(r, value_field)))

    stats: list[GroupStats] = []
    for group_val, values in sorted(groups.items()):
        stats.append(
            GroupStats(
                group_value=group_val,
                mean=round(statistics.mean(values), 2),
                min_val=round(min(values), 2),
                max_val=round(max(values), 2),
                count=len(values),
            )
        )

    return AggregationResult(group_key=group_field, groups=stats, metric_count=len(stats))


@task
def build_cross_tab(
    records: list[WeatherObservation],
    row_field: str,
    col_field: str,
    value_field: str,
) -> CrossTab:
    """Build a cross-tabulation of mean values.

    Args:
        records: Input records.
        row_field: Field for row labels.
        col_field: Field for column labels.
        value_field: Numeric field to aggregate (mean).

    Returns:
        CrossTab.
    """
    cells: dict[tuple[str, str], list[float]] = {}
    row_labels_set: set[str] = set()
    col_labels_set: set[str] = set()

    for r in records:
        row = str(getattr(r, row_field))
        col = str(getattr(r, col_field))
        row_labels_set.add(row)
        col_labels_set.add(col)
        cells.setdefault((row, col), []).append(float(getattr(r, value_field)))

    row_labels = sorted(row_labels_set)
    col_labels = sorted(col_labels_set)

    matrix: list[list[float]] = []
    for row in row_labels:
        row_values: list[float] = []
        for col in col_labels:
            vals = cells.get((row, col), [])
            row_values.append(round(statistics.mean(vals), 2) if vals else 0.0)
        matrix.append(row_values)

    return CrossTab(row_labels=row_labels, col_labels=col_labels, matrix=matrix)


@task
def aggregation_summary(
    station_agg: AggregationResult,
    date_agg: AggregationResult,
    cross_tab: CrossTab,
    record_count: int,
) -> AggregationReport:
    """Combine all aggregation results.

    Args:
        station_agg: Aggregation by station.
        date_agg: Aggregation by date.
        cross_tab: Cross-tabulation.
        record_count: Total records processed.

    Returns:
        AggregationReport.
    """
    return AggregationReport(
        by_station=station_agg,
        by_date=date_agg,
        cross_tab=cross_tab,
        record_count=record_count,
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="084_statistical_aggregation", log_prints=True)
def statistical_aggregation_flow(
    stations: list[str] | None = None,
    days: int = 7,
) -> AggregationReport:
    """Run the statistical aggregation pipeline.

    Args:
        stations: Station names. Defaults to sample stations.
        days: Number of days of data.

    Returns:
        AggregationReport.
    """
    if stations is None:
        stations = ["North", "South", "East", "West"]

    records = generate_weather_data(stations, days)

    # Fan-out: 3 independent aggregations
    station_future = aggregate_by_group.submit(records, "station", "temperature")
    date_future = aggregate_by_group.submit(records, "day", "temperature")
    cross_tab_future = build_cross_tab.submit(records, "station", "day", "temperature")

    station_agg = station_future.result()
    date_agg = date_future.result()
    cross_tab = cross_tab_future.result()

    report = aggregation_summary(station_agg, date_agg, cross_tab, len(records))
    print(f"Aggregation complete: {report.record_count} records, {station_agg.metric_count} stations")
    return report


if __name__ == "__main__":
    statistical_aggregation_flow()
