from django.core.management.base import BaseCommand
from django_celery_beat.models import PeriodicTask, IntervalSchedule, CrontabSchedule

class Command(BaseCommand):
    help = "Register ETL periodic task for Celery Beat"

    def handle(self, *args, **kwargs):
        # Crontab config (every day 8:10 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='10',
            hour='8',
            day_of_week='*',
            day_of_month='*',
            month_of_year='*',
        )
        
        # Register load_last_night task for crontab
        PeriodicTask.objects.update_or_create(
            name='load_last_night_daily_task_at_8_10am_',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.load_last_night',
            },
        )
        self.stdout.write("Success register load_last_night ETL Task.")

        # Crontab config (every day 8:20 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='20',
            hour='8',
            day_of_week='*',
            day_of_month='*',
            month_of_year='*',
        )
        # Register load_last_week task for crontab
        PeriodicTask.objects.update_or_create(
            name='load_last_week_daily_task_at_8_20am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.load_last_week',
            },
        )
        self.stdout.write("Success register load_last_week ETL Task.")

        # Crontab config (every day 8:30 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='30',
            hour='8',
            day_of_week='*',
            day_of_month='*',
            month_of_year='*',
        )
        # Register load_last_month task for crontab
        PeriodicTask.objects.update_or_create(
            name='load_last_month_daily_task_at_8_30am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.load_last_month',
            },
        )
        self.stdout.write("Success register load_last_month ETL Task.")

        # Crontab config (every first day on month 00:00 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='0',
            hour='0',
            day_of_week='*',
            day_of_month='1',
            month_of_year='*',
        )
        # Register historical_bortle task for crontab
        PeriodicTask.objects.update_or_create(
            name='historical_bortle_every_first_day_month_task_at_00_00am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.historical_bortle',
            },
        )
        self.stdout.write("Success register historical_bortle ETL Task.")

        # Crontab config (every first day on month 00:10 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='10',
            hour='0',
            day_of_week='*',
            day_of_month='1',
            month_of_year='*',
        )
        # Register historical_stddev_magnitude task for crontab
        PeriodicTask.objects.update_or_create(
            name='historical_stddev_magnitude_every_first_day_month_task_at_00_10am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.historical_stddev_magnitude',
            },
        )
        self.stdout.write("Success register historical_stddev_magnitude ETL Task.")

        # Crontab config (every first day on month 00:20 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='20',
            hour='0',
            day_of_week='*',
            day_of_month='1',
            month_of_year='*',
        )
        # Register historical_median_magnitude_clear task for crontab
        PeriodicTask.objects.update_or_create(
            name='historical_median_magnitude_clear_every_first_day_month_task_at_00_20am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.historical_median_magnitude_clear',
            },
        )
        self.stdout.write("Success register historical_median_magnitude_clear ETL Task.")

        # Crontab config (every first day on month 00:30 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='30',
            hour='0',
            day_of_week='*',
            day_of_month='1',
            month_of_year='*',
        )
        # Register historical_stddev_magnitude_clear task for crontab
        PeriodicTask.objects.update_or_create(
            name='historical_stddev_magnitude_clear_every_first_day_month_task_at_00_30am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.historical_stddev_magnitude_clear',
            },
        )
        self.stdout.write("Success register historical_stddev_magnitude_clear ETL Task.")

        # Crontab config (every first day on month 00:40 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='40',
            hour='0',
            day_of_week='*',
            day_of_month='1',
            month_of_year='*',
        )
        # Register historical_median_magnitude_covered task for crontab
        PeriodicTask.objects.update_or_create(
            name='historical_median_magnitude_covered_every_first_day_month_task_at_00_40am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.historical_median_magnitude_covered',
            },
        )
        self.stdout.write("Success register historical_median_magnitude_covered ETL Task.")

        # Crontab config (every first day on month 00:50 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='50',
            hour='0',
            day_of_week='*',
            day_of_month='1',
            month_of_year='*',
        )
        # Register historical_stddev_magnitude_covered task for crontab
        PeriodicTask.objects.update_or_create(
            name='historical_stddev_magnitude_covered_every_first_day_month_task_at_00_50am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.historical_stddev_magnitude_covered',
            },
        )
        self.stdout.write("Success register historical_stddev_magnitude_covered ETL Task.")

        # Crontab config (every first day on month 01:00 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='0',
            hour='1',
            day_of_week='*',
            day_of_month='1',
            month_of_year='*',
        )
        # Register historical_median_magnitude_cloudy task for crontab
        PeriodicTask.objects.update_or_create(
            name='historical_median_magnitude_cloudy_every_first_day_month_task_at_01_00am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.historical_median_magnitude_cloudy',
            },
        )
        self.stdout.write("Success register historical_median_magnitude_cloudy ETL Task.")

        # Crontab config (every first day on month 01:10 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='10',
            hour='1',
            day_of_week='*',
            day_of_month='1',
            month_of_year='*',
        )
        # Register historical_stddev_magnitude_cloudy task for crontab
        PeriodicTask.objects.update_or_create(
            name='historical_stddev_magnitude_cloudy_every_first_day_month_task_at_01_10am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.historical_stddev_magnitude_cloudy',
            },
        )
        self.stdout.write("Success register historical_stddev_magnitude_cloudy ETL Task.")

        # Crontab config (every first day on month 01:20 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='20',
            hour='1',
            day_of_week='*',
            day_of_month='1',
            month_of_year='*',
        )
        # Register historical_total_measurements_month task for crontab
        PeriodicTask.objects.update_or_create(
            name='historical_total_measurements_month_every_first_day_month_task_at_01_20am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.historical_total_measurements_month',
            },
        )
        self.stdout.write("Success register historical_total_measurements_month ETL Task.")

        # Crontab config (every first day on month 01:30 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='30',
            hour='1',
            day_of_week='*',
            day_of_month='1',
            month_of_year='*',
        )
        # Register historical_total_measurements_month task for crontab
        PeriodicTask.objects.update_or_create(
            name='historical_total_measurements_month_clear_every_first_day_month_task_at_01_30am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.historical_total_measurements_month_clear',
            },
        )
        self.stdout.write("Success register historical_total_measurements_month_clear ETL Task.")

        # Crontab config (every first day on month 01:40 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='40',
            hour='1',
            day_of_week='*',
            day_of_month='1',
            month_of_year='*',
        )
        # Register historical_total_measurements_month_covered task for crontab
        PeriodicTask.objects.update_or_create(
            name='historical_total_measurements_month_covered_every_first_day_month_task_at_01_40am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.historical_total_measurements_month_covered',
            },
        )
        self.stdout.write("Success register historical_total_measurements_month_covered ETL Task.")

        # Crontab config (every first day on month 01:50 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='50',
            hour='1',
            day_of_week='*',
            day_of_month='1',
            month_of_year='*',
        )
        # Register historical_total_measurements_month_cloudy task for crontab
        PeriodicTask.objects.update_or_create(
            name='historical_total_measurements_month_cloudy_every_first_day_month_task_at_01_50am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.historical_total_measurements_month_cloudy',
            },
        )
        self.stdout.write("Success register historical_total_measurements_month_cloudy ETL Task.")

        # Crontab config (every first day on month 02:00 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='0',
            hour='2',
            day_of_week='*',
            day_of_month='1',
            month_of_year='*',
        )
        # Register historical_percentage_measurements_month task for crontab
        PeriodicTask.objects.update_or_create(
            name='historical_percentage_measurements_month_every_first_day_month_task_at_02_00am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.historical_percentage_measurements_month',
            },
        )
        self.stdout.write("Success register historical_percentage_measurements_month ETL Task.")

        # Crontab config (every first day on month 02:10 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='10',
            hour='2',
            day_of_week='*',
            day_of_month='1',
            month_of_year='*',
        )
        # Register historical_percentage_measurements_month_clear task for crontab
        PeriodicTask.objects.update_or_create(
            name='historical_percentage_measurements_month_clear_every_first_day_month_task_at_02_10am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.historical_percentage_measurements_month_clear',
            },
        )
        self.stdout.write("Success register historical_percentage_measurements_month_clear ETL Task.")

        # Crontab config (every first day on month 02:20 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='20',
            hour='2',
            day_of_week='*',
            day_of_month='1',
            month_of_year='*',
        )
        # Register historical_percentage_measurements_month_covered task for crontab
        PeriodicTask.objects.update_or_create(
            name='historical_percentage_measurements_month_covered_every_first_day_month_task_at_02_20am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.historical_percentage_measurements_month_covered',
            },
        )
        self.stdout.write("Success register historical_percentage_measurements_month_covered ETL Task.")

        # Crontab config (every first day on month 02:30 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='30',
            hour='2',
            day_of_week='*',
            day_of_month='1',
            month_of_year='*',
        )
        # Register historical_percentage_measurements_month_cloudy task for crontab
        PeriodicTask.objects.update_or_create(
            name='historical_percentage_measurements_month_cloudy_every_first_day_month_task_at_02_30am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.historical_percentage_measurements_month_cloudy',
            },
        )
        self.stdout.write("Success register historical_percentage_measurements_month_cloudy ETL Task.")

        # Crontab config (every first day on month 03:00 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='0',
            hour='3',
            day_of_week='*',
            day_of_month='1',
            month_of_year='*',
        )
        # Register upload_month_data by date task for crontab
        PeriodicTask.objects.update_or_create(
            name='upload_month_data_by_date_task_at_03_0am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.upload_month_data_by_date',
            },
        )
        self.stdout.write("Success register upload_month_data_by_date ETL Task.")

        # Crontab config (every first day on month 03:30 AM)
        crontab_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='30',
            hour='3',
            day_of_week='*',
            day_of_month='1',
            month_of_year='*',
        )
        # Register upload_month_data by id task for crontab
        PeriodicTask.objects.update_or_create(
            name='upload_month_data_by_id_task_at_03_0am',
            defaults={
                'crontab': crontab_schedule,
                'task': 'apps.etl.tasks.upload_month_data_by_id',
            },
        )
        self.stdout.write("Success register upload_month_data_by_id ETL Task.")