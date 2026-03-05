#!/usr/bin/python
# -*- coding: utf-8 -*-

from celery import shared_task
from datetime import datetime, timedelta
from .processes import ETLProcessing
import pytz

etl=ETLProcessing()

def get_period():
    now_utc = datetime.utcnow()
    santiago_timezone = pytz.timezone('America/Santiago')
    now_santiago = pytz.utc.localize(now_utc).astimezone(santiago_timezone)
    month=now_santiago.month
    year=now_santiago.year
    date_str = str(year)+"-"+str(month)+"-01 00:00:00"
    period = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    santiago_timezone = pytz.timezone('America/Santiago')
    to_date = pytz.utc.localize(period).astimezone(santiago_timezone)
    from_date = (to_date-timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")+'Z'
    to_date=to_date.strftime("%Y-%m-%dT%H:%M:%S")+'Z'
    now_utc = datetime.utcnow()
    santiago_timezone = pytz.timezone('America/Santiago')
    now = pytz.utc.localize(now_utc).astimezone(santiago_timezone)
    return from_date, to_date, now, period

@shared_task()
def load_last_night():
    etl.load_last_night(14,"12:00:00",'W')
    etl.load_last_night(14,"12:00:00",'4C')
    
@shared_task()
def load_last_week():
    etl.load_last_week(7,"12:00:00")

@shared_task()
def load_last_month():
    etl.load_last_month(30,"12:00:00")

@shared_task()
def light_pollution_indicator():
    etl.light_pollution_indicator()

@shared_task()
def historical_bortle():
    from_date, to_date, now, period=get_period()
    etl.historical_bortle(from_date, to_date, now, period)

@shared_task()
def historical_stddev_magnitude():
    from_date, to_date, now, period=get_period()
    etl.historical_stddev_magnitude(from_date, to_date, now, period)

@shared_task()
def historical_median_magnitude_clear():
    from_date, to_date, now, period=get_period()
    etl.historical_median_magnitude_clear(from_date, to_date, now, period)

@shared_task()
def historical_stddev_magnitude_clear():
    from_date, to_date, now, period=get_period()
    etl.historical_stddev_magnitude_clear(from_date, to_date, now, period)

@shared_task()
def historical_median_magnitude_covered():
    from_date, to_date, now, period=get_period()
    etl.historical_median_magnitude_covered(from_date, to_date, now, period)

@shared_task()
def historical_stddev_magnitude_covered():
    from_date, to_date, now, period=get_period()
    etl.historical_stddev_magnitude_covered(from_date, to_date, now, period)

@shared_task()
def historical_median_magnitude_cloudy():
    from_date, to_date, now, period=get_period()
    etl.historical_median_magnitude_cloudy(from_date, to_date, now, period)

@shared_task()
def historical_stddev_magnitude_cloudy():
    from_date, to_date, now, period=get_period()
    etl.historical_stddev_magnitude_cloudy(from_date, to_date, now, period)

@shared_task()
def historical_percentage_measurements_month():
    from_date, to_date, now, period=get_period()
    etl.historical_percentage_measurements_month(from_date, to_date, now, period)

@shared_task()
def historical_total_measurements_month():
    from_date, to_date, now, period=get_period()
    etl.historical_total_measurements_month(from_date, to_date, now, period)

@shared_task()
def historical_percentage_measurements_month_clear():
    from_date, to_date, now, period=get_period()
    etl.historical_percentage_measurements_month_clear(from_date, to_date, now, period)

@shared_task()
def historical_total_measurements_month_clear():
    from_date, to_date, now, period=get_period()
    etl.historical_total_measurements_month_clear(from_date, to_date, now, period)

@shared_task()
def historical_percentage_measurements_month_covered():
    from_date, to_date, now, period=get_period()
    etl.historical_percentage_measurements_month_covered(from_date, to_date, now, period)

@shared_task()
def historical_total_measurements_month_covered():
    from_date, to_date, now, period=get_period()
    etl.historical_total_measurements_month_covered(from_date, to_date, now, period)

@shared_task()
def historical_percentage_measurements_month_cloudy():
    from_date, to_date, now, period=get_period()
    etl.historical_percentage_measurements_month_cloudy(from_date, to_date, now, period)

@shared_task()
def historical_total_measurements_month_cloudy():
    from_date, to_date, now, period=get_period()
    etl.historical_total_measurements_month_cloudy(from_date, to_date, now, period)

@shared_task()
def upload_month_data_by_date():
    from_date, to_date, now, period=get_period()
    etl.upload_month_data_by_date(from_date, to_date, now, period)

@shared_task()
def upload_month_data_by_id():
    from_date, to_date, now, period=get_period()
    etl.upload_month_data_by_id(from_date, to_date, now, period)