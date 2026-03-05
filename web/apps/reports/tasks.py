#!/usr/bin/python
# -*- coding: utf-8 -*-
from celery import shared_task
from apps.dashboard.models import TessW
from .reports import ReportsProcessing

report=ReportsProcessing()

@shared_task
def send_month_report():
    tessws=TessW.objects.filter(institution_email_verification=True)
    for tessw in tessws:
        report.send_month_report(tessw.id)

@shared_task
def observation_time_report():
    report.observation_time_report()