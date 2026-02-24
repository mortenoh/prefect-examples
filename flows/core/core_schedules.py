"""Schedules.

Demonstrate schedule types available for Prefect deployments.

Airflow equivalent: DAG schedule_interval (cron, timedelta, timetable).
Prefect approach:    CronSchedule, IntervalSchedule, RRuleSchedule —
                     passed to flow.serve(schedule=...) or flow.deploy().

Schedule classes (for reference):
    from prefect.client.schemas.schedules import (
        CronSchedule,
        IntervalSchedule,
        RRuleSchedule,
    )
"""

import datetime

from prefect import flow, task


@task
def run_daily_report(report_date: str) -> str:
    """Run a daily report for a given date.

    In production, deploy with CronSchedule("0 6 * * *") to run
    at 6:00 AM daily.

    Args:
        report_date: The date string for the report.

    Returns:
        A report summary.
    """
    msg = f"Daily report for {report_date} — completed"
    print(msg)
    return msg


@task
def run_interval_check() -> str:
    """Run a periodic health check.

    In production, deploy with IntervalSchedule(interval=timedelta(minutes=15))
    to run every 15 minutes.

    Returns:
        A check result string.
    """
    ts = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S")
    msg = f"Interval check at {ts} — all systems OK"
    print(msg)
    return msg


@task
def run_custom_schedule_job() -> str:
    """Run a job on a custom recurrence schedule.

    In production, deploy with RRuleSchedule(rrule="FREQ=WEEKLY;BYDAY=MO,WE,FR")
    to run on Monday, Wednesday, and Friday.

    Returns:
        A job result string.
    """
    msg = "Custom schedule job — completed"
    print(msg)
    return msg


@flow(name="core_daily_report", log_prints=True)
def daily_report_flow(report_date: str | None = None) -> None:
    """Daily report flow — in production, served with CronSchedule.

    Example deployment:
        daily_report_flow.serve(
            name="daily-report",
            cron="0 6 * * *",
        )

    Args:
        report_date: Optional report date; defaults to today.
    """
    if report_date is None:
        report_date = datetime.date.today().isoformat()
    run_daily_report(report_date)


@flow(name="core_interval_check", log_prints=True)
def interval_check_flow() -> None:
    """Interval check flow — in production, served with IntervalSchedule.

    Example deployment:
        interval_check_flow.serve(
            name="interval-check",
            interval=900,  # 15 minutes in seconds
        )
    """
    run_interval_check()


@flow(name="core_custom_schedule", log_prints=True)
def custom_schedule_flow() -> None:
    """Custom schedule flow — in production, served with RRuleSchedule.

    Example deployment:
        custom_schedule_flow.serve(
            name="custom-schedule",
            rrule="FREQ=WEEKLY;BYDAY=MO,WE,FR",
        )
    """
    run_custom_schedule_job()


@flow(name="core_schedules", log_prints=True)
def schedules_flow() -> None:
    """Run all schedule demonstration flows."""
    daily_report_flow()
    interval_check_flow()
    custom_schedule_flow()


if __name__ == "__main__":
    schedules_flow()
