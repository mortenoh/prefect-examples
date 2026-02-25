"""Financial Time Series.

Log returns, rolling window volatility, cross-currency correlation matrix,
and anomaly detection based on standard deviation thresholds.

Airflow equivalent: Currency analysis with log returns and volatility (DAG 089).
Prefect approach:    Simulate daily exchange rates, compute log returns, rolling
                     volatility, correlation matrix, and detect anomalies.
"""

import math
import statistics

from dotenv import load_dotenv
from prefect import flow, task
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class LogReturn(BaseModel):
    """A single log return observation."""

    day: int
    return_value: float


class RateRecord(BaseModel):
    """Daily exchange rate observation."""

    day: int
    currency: str
    rate: float


class VolatilityResult(BaseModel):
    """Annualized volatility for a currency."""

    currency: str
    annualized_vol: float
    window_size: int


class MarketEvent(BaseModel):
    """An anomalous market event."""

    day: int
    currency: str
    return_value: float
    z_score: float


class TimeSeriesReport(BaseModel):
    """Summary time series analysis report."""

    volatilities: list[VolatilityResult]
    correlations: dict[str, float]
    events: list[MarketEvent]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def simulate_exchange_rates(currencies: list[str], days: int) -> list[RateRecord]:
    """Generate deterministic exchange rate data using a seeded walk.

    Args:
        currencies: Currency codes.
        days: Number of trading days.

    Returns:
        List of RateRecord.
    """
    records: list[RateRecord] = []
    for ci, currency in enumerate(currencies):
        rate = 1.0 + ci * 0.3
        seed = 42 + ci * 7
        for day in range(days):
            # Deterministic pseudo-random walk
            x = ((seed + day * 17 + ci * 31) * 2654435761) % (2**32)
            change = (x / (2**32) - 0.5) * 0.02  # +/- 1% daily change
            rate = rate * (1.0 + change)
            records.append(RateRecord(day=day, currency=currency, rate=round(rate, 6)))
    print(f"Simulated {len(records)} rate records for {len(currencies)} currencies")
    return records


@task
def compute_log_returns(records: list[RateRecord]) -> dict[str, list[LogReturn]]:
    """Compute log returns for each currency.

    Args:
        records: Rate records.

    Returns:
        Dict mapping currency to list of LogReturn.
    """
    by_currency: dict[str, list[RateRecord]] = {}
    for r in records:
        by_currency.setdefault(r.currency, []).append(r)

    returns: dict[str, list[LogReturn]] = {}
    for currency, rates in by_currency.items():
        sorted_rates = sorted(rates, key=lambda r: r.day)
        currency_returns: list[LogReturn] = []
        for i in range(1, len(sorted_rates)):
            if sorted_rates[i - 1].rate > 0 and sorted_rates[i].rate > 0:
                log_ret = math.log(sorted_rates[i].rate / sorted_rates[i - 1].rate)
                currency_returns.append(LogReturn(day=sorted_rates[i].day, return_value=round(log_ret, 8)))
        returns[currency] = currency_returns
    return returns


@task
def rolling_volatility(
    returns_by_currency: dict[str, list[LogReturn]],
    window: int,
    annualize_factor: float,
) -> list[VolatilityResult]:
    """Compute rolling volatility, annualized.

    Args:
        returns_by_currency: Log returns per currency.
        window: Rolling window size.
        annualize_factor: Factor to annualize (e.g., sqrt(252)).

    Returns:
        List of VolatilityResult.
    """
    results: list[VolatilityResult] = []
    for currency, rets in returns_by_currency.items():
        if len(rets) < window:
            results.append(VolatilityResult(currency=currency, annualized_vol=0.0, window_size=window))
            continue
        # Use the last window of returns
        last_window = [r.return_value for r in rets[-window:]]
        vol = statistics.stdev(last_window) * annualize_factor
        results.append(VolatilityResult(currency=currency, annualized_vol=round(vol, 6), window_size=window))
    return results


@task
def correlation_matrix(returns_by_currency: dict[str, list[LogReturn]]) -> dict[str, float]:
    """Compute pairwise Pearson correlation between currencies.

    Args:
        returns_by_currency: Log returns per currency.

    Returns:
        Dict mapping "CUR_A:CUR_B" to correlation value.
    """
    currencies = sorted(returns_by_currency.keys())
    result: dict[str, float] = {}

    for i, cur_a in enumerate(currencies):
        for cur_b in currencies[i:]:
            if cur_a == cur_b:
                result[f"{cur_a}:{cur_b}"] = 1.0
                continue
            rets_a = {r.day: r.return_value for r in returns_by_currency[cur_a]}
            rets_b = {r.day: r.return_value for r in returns_by_currency[cur_b]}
            common_days = sorted(set(rets_a.keys()) & set(rets_b.keys()))
            if len(common_days) < 3:
                result[f"{cur_a}:{cur_b}"] = 0.0
                continue
            x = [rets_a[d] for d in common_days]
            y = [rets_b[d] for d in common_days]
            r_val = _pearson(x, y)
            result[f"{cur_a}:{cur_b}"] = round(r_val, 4)
    return result


@task
def detect_anomalies(
    returns_by_currency: dict[str, list[LogReturn]],
    threshold_stdev: float,
) -> list[MarketEvent]:
    """Detect anomalous returns exceeding threshold standard deviations.

    Args:
        returns_by_currency: Log returns per currency.
        threshold_stdev: Number of standard deviations for anomaly threshold.

    Returns:
        List of MarketEvent.
    """
    events: list[MarketEvent] = []
    for currency, rets in returns_by_currency.items():
        if len(rets) < 3:
            continue
        values = [r.return_value for r in rets]
        mean_r = statistics.mean(values)
        stdev_r = statistics.stdev(values)
        if stdev_r == 0:
            continue
        for r in rets:
            z = (r.return_value - mean_r) / stdev_r
            if abs(z) > threshold_stdev:
                events.append(
                    MarketEvent(
                        day=r.day,
                        currency=currency,
                        return_value=round(r.return_value, 8),
                        z_score=round(z, 4),
                    )
                )
    print(f"Detected {len(events)} anomalies across all currencies")
    return events


@task
def time_series_summary(
    volatilities: list[VolatilityResult],
    correlations: dict[str, float],
    events: list[MarketEvent],
) -> TimeSeriesReport:
    """Build the final time series report.

    Args:
        volatilities: Volatility results.
        correlations: Correlation matrix.
        events: Market events.

    Returns:
        TimeSeriesReport.
    """
    return TimeSeriesReport(volatilities=volatilities, correlations=correlations, events=events)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pearson(x: list[float], y: list[float]) -> float:
    """Compute Pearson correlation coefficient."""
    n = len(x)
    if n < 2:
        return 0.0
    mean_x = statistics.mean(x)
    mean_y = statistics.mean(y)
    num = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y, strict=True))
    den_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
    den_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))
    if den_x == 0 or den_y == 0:
        return 0.0
    return num / (den_x * den_y)


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="analytics_financial_time_series", log_prints=True)
def financial_time_series_flow(
    currencies: list[str] | None = None,
    days: int = 100,
) -> TimeSeriesReport:
    """Run the financial time series analysis pipeline.

    Args:
        currencies: Currency codes.
        days: Number of trading days.

    Returns:
        TimeSeriesReport.
    """
    if currencies is None:
        currencies = ["EUR", "GBP", "JPY", "CHF", "SEK", "NOK"]

    rates = simulate_exchange_rates(currencies, days)
    returns = compute_log_returns(rates)
    vols = rolling_volatility(returns, window=20, annualize_factor=math.sqrt(252))
    corrs = correlation_matrix(returns)
    events = detect_anomalies(returns, threshold_stdev=2.0)
    report = time_series_summary(vols, corrs, events)
    print(f"Time series analysis complete: {len(vols)} currencies, {len(events)} events")
    return report


if __name__ == "__main__":
    load_dotenv()
    financial_time_series_flow()
