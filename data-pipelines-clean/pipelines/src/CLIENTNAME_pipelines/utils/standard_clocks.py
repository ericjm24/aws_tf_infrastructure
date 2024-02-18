from datetime import datetime, timedelta
from prefect.schedules.clocks import IntervalClock, CronClock


def daily_clock(shift=0, **kwargs):
    c = IntervalClock(
        interval=timedelta(days=1),
        start_date=datetime.fromisoformat("2022-01-01 05:00:00")
        + timedelta(minutes=shift),
        parameter_defaults=kwargs,
    )
    return c


def hourly_clock(shift=0, **kwargs):
    c = IntervalClock(
        interval=timedelta(hours=1),
        start_date=datetime.fromisoformat("2022-01-01 05:00:00")
        + timedelta(minutes=shift),
        parameter_defaults=kwargs,
    )
    return c


def weekly_clock(day_of_week=0, hour=0, minute=0, **kwargs):
    hour += minute // 60
    minute = minute % 60
    day_of_week += hour // 24
    hour = hour % 24
    day_of_week = day_of_week % 7
    cron = f"{minute} {hour} * * {day_of_week}"
    c = CronClock(cron=cron, parameter_defaults=kwargs)
    return c
