from django.core.management.base import BaseCommand
from django_celery_beat.models import PeriodicTask, IntervalSchedule, CrontabSchedule

class Command(BaseCommand):
    help = "Register Reports periodic task for Celery Beat"

    def handle(self, *args, **kwargs):
        # Crontab config (every monday 8:00 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='0',
            hour='8',
            day_of_week='1',
            day_of_month='*',
            month_of_year='*',
        )
        # Register observation_time_report task for crontab
        PeriodicTask.objects.update_or_create(
            name='observation_time_report_first_monday_task_at_08_00am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.reports.tasks.observation_time_report',
            },
        )
        self.stdout.write("Success register observation_time_report Reports Task.")

        # Crontab config (every first day on month 08:40 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='40',
            hour='8',
            day_of_week='*',
            day_of_month='*',
            month_of_year='1',
        )
        # Register send_month_report task for crontab
        PeriodicTask.objects.update_or_create(
            name='send_month_report_every_first_day_month_task_at_08_40am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.reports.tasks.send_month_report',
            },
        )
        self.stdout.write("Success register send_month_report Reports Task.")